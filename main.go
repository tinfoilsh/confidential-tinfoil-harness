// Command confidential-tinfoil-harness serves a streaming, OpenAI-shaped agent endpoint that
// seals every model turn and tool call to enclaves it attests against the repos
// pinned in enclave.go. It holds no state between requests and no credentials.
package main

import (
	"context"
	"flag"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const maxRequestBody = 8 << 20

// harness is immutable once built: every enclave it may reach was attested
// before it began serving, so a request shares nothing but read-only state.
type harness struct {
	gateway       string
	toolsEndpoint string
	toolsClient   *http.Client
}

func main() {
	addr := flag.String("addr", ":8081", "listen address")
	gateway := flag.String("gateway", env("TINFOIL_GATEWAY_URL", "https://gateway.tinfoil.sh"), "tinfoil-gateway base URL")
	toolsHost := flag.String("tools-enclave", env("TINFOIL_TOOLS_ENCLAVE", defaultToolsEnclave), "host of the enclave serving web_search and web_fetch")
	flag.Parse()

	gw := strings.TrimRight(*gateway, "/")
	// Attested at startup: every request advertises these tools, so a harness
	// that cannot reach them cannot serve.
	toolsClient, err := attest(*toolsHost, toolsRepo, "")
	if err != nil {
		slog.Error("verify tools enclave", "enclave", *toolsHost, "error", err)
		os.Exit(1)
	}
	toolsClient.Transport = &callerAuth{inner: toolsClient.Transport}
	if err := attestCatalog(gw); err != nil {
		slog.Error("verify model enclaves", "gateway", gw, "error", err)
		os.Exit(1)
	}
	h := &harness{gateway: gw, toolsEndpoint: "https://" + *toolsHost + "/mcp", toolsClient: toolsClient}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") })
	mux.HandleFunc("POST /v1/chat/completions", h.chat)

	// No WriteTimeout: an answer streams for as long as the loop takes.
	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}()

	slog.Info("listening", "addr", *addr, "gateway", gw, "tools", *toolsHost, "models", served)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		slog.Error("serve", "error", err)
		os.Exit(1)
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
