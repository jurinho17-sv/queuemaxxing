// Command queued runs the queue as an HTTP server.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/jurinho17-sv/queuemaxxing/internal/httpapi"
	"github.com/jurinho17-sv/queuemaxxing/internal/queue"
)

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	dataDir := flag.String("data", "./data", "directory holding queue logs")
	flag.Parse()

	// Opening the broker replays every queue's log, so by the time the listener
	// is up the server is back in exactly the state it crashed in.
	broker, err := queue.NewBroker(*dataDir)
	if err != nil {
		log.Fatalf("open data dir %s: %v", *dataDir, err)
	}
	defer broker.Close()

	for _, s := range broker.List() {
		log.Printf("recovered queue %q (%s, priority=%v, delay=%ds): %d ready, %d delayed",
			s.Name, s.Policy.Order, s.Policy.Priority, s.Policy.DelaySeconds, s.Ready, s.Delayed)
	}

	srv := &http.Server{
		Addr:         *addr,
		Handler:      httpapi.New(broker),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("listening on %s, data in %s", *addr, *dataDir)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve: %v", err)
		}
	}()

	<-ctx.Done()
	// Shutdown only has to drain in-flight requests. Nothing needs flushing:
	// every accepted message was fsynced before its response went out.
	log.Print("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}
