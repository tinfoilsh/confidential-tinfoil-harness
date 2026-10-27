// Package server wires the harness and serves its HTTP API.
package server

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

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/archives"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/attach"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/engine"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/inference"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/store"
)

type harness struct {
	controlplane string
	auth         *auth.Authenticator
	store        *store.Store
	models       *inference.Models
	attachments  *attach.Service
	archives     *archives.Service
	engine       *engine.Engine
}

// listenAddr is the shim's upstream port in tinfoil-config.yml, which also
// sets every host this harness reaches.
const listenAddr = ":8081"

// Run starts the chat server and waits for shutdown.
func Run() {
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
	engine.AttestWebSearch(ctx, usageSecret)
	scratch, err := archives.MeasuredScratch()
	if err != nil {
		slog.Error("prepare the temporary directory", "error", err)
		os.Exit(1)
	}
	h := &harness{models: &inference.Models{Gateway: gw, Controlplane: controlplane},
		controlplane: controlplane,
		store:        &store.Store{}}

	if err := h.bootChat(ctx, usageSecret); err != nil {
		slog.Error("initialize chat service", "error", err)
		os.Exit(1)
	}
	h.attachments = &attach.Service{Auth: h.auth, Controlplane: h.controlplane, Models: h.models, Store: h.store}
	h.store.Repair = h.attachments
	h.engine = &engine.Engine{Safeguards: engine.NewSubmitter(os.Getenv("SAFEGUARDS_URL")), Store: h.store, Models: h.models, Attachments: h.attachments, Auth: h.auth}
	h.archives = &archives.Service{Scratch: scratch, CPClient: &http.Client{Transport: &auth.CallerTransport{Inner: http.DefaultTransport}}, Store: h.store, Attachments: h.attachments, Auth: h.auth, Controlplane: h.controlplane}

	srv := &http.Server{Addr: listenAddr, Handler: h.routes(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: time.Minute}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	stopped := make(chan struct{})
	go func() {
		<-stop
		defer close(stopped)
		h.engine.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
		h.engine.Wait(ctx)
	}()

	slog.Info("listening", "addr", listenAddr, "gateway", gw, "models", inference.Served(h.models.Offer.Get()), "safeguards", h.engine.Safeguards != nil)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		slog.Error("serve", "error", err)
		os.Exit(1)
	}
	<-stopped
}
