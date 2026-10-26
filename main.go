// Command confidential-tinfoil-harness runs Tinfoil Chat conversations whose
// model turns and tool calls all go to enclaves it attests against a pinned repo.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	_ "time/tzdata"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
)

type harness struct {
	controlplane string
	cpClient     *http.Client
	auth         *auth.Authenticator
	safeguards   *submitter // nil unless SAFEGUARDS_URL names the sidecar
	live         liveRuns
	scratch      *scratchSpace
	store        *store.Store
	models       *models
}

// listenAddr is the shim's upstream port in tinfoil-config.yml, which also
// sets every host this harness reaches.
const listenAddr = ":8081"

func main() {
	usageSecret := os.Getenv("USAGE_CONTEXT_SECRET")
	if usageSecret == "" {
		slog.Error("USAGE_CONTEXT_SECRET is required: without it every turn of a run bills as its own request")
		os.Exit(1)
	}
	gw, controlplane := strings.TrimRight(os.Getenv("TINFOIL_GATEWAY_URL"), "/"), strings.TrimRight(os.Getenv("TINFOIL_CONTROLPLANE_URL"), "/")
	if gw == "" || controlplane == "" {
		slog.Error("TINFOIL_GATEWAY_URL and TINFOIL_CONTROLPLANE_URL are required")
		os.Exit(1)
	}

	ctx, shutdown := context.WithCancel(context.Background())
	defer shutdown()
	attestWebSearch(ctx, usageSecret)
	scratch, err := measuredScratch()
	if err != nil {
		slog.Error("prepare the temporary directory", "error", err)
		os.Exit(1)
	}
	h := &harness{models: &models{gateway: gw, controlplane: controlplane},
		controlplane: controlplane,
		cpClient:     &http.Client{Transport: &auth.CallerTransport{Inner: http.DefaultTransport}},
		safeguards:   newSubmitter(os.Getenv("SAFEGUARDS_URL")),
		scratch:      scratch,
		store:        &store.Store{}}
	h.store.Repair = h

	if err := h.bootChat(ctx, usageSecret); err != nil {
		slog.Error("initialize chat service", "error", err)
		os.Exit(1)
	}

	srv := &http.Server{Addr: listenAddr, Handler: h.routes(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: time.Minute}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	stopped := make(chan struct{})
	go func() {
		<-stop
		defer close(stopped)
		h.stopRuns()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
		h.waitRuns(ctx)
	}()

	slog.Info("listening", "addr", listenAddr, "gateway", gw, "models", served(h.models.offer.get()), "safeguards", h.safeguards != nil)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		slog.Error("serve", "error", err)
		os.Exit(1)
	}
	<-stopped
}

// Web search is opt-in per run, so an enclave that is not configured or does
// not verify is skipped, not fatal.
func attestWebSearch(ctx context.Context, usageSecret string) {
	webSearch.enclave = os.Getenv("TINFOIL_TOOLS_ENCLAVE")
	if webSearch.enclave == "" {
		slog.Warn("no web search enclave is configured")
		return
	}
	client, err := attest(ctx, webSearch.enclave, webSearch.repo, usageSecret)
	if err != nil {
		slog.Warn("tool enclave did not verify", "enclave", webSearch.enclave, "error", err)
		return
	}
	webSearch.client = client
	slog.Info("tool enclave verified", "enclave", webSearch.enclave)
}

// instanceID names this process, which alone holds its live runs. Clients
// send it back with requests about a run, so that a deployment of several
// can route them here.
var instanceID = api.Token()
