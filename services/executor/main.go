package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"

	"github.com/automata-tech/distributed-systems-interview/services/common/storage"
)

func required(name string) string {
	v := os.Getenv(name)
	if v == "" {
		log.Fatalf("%s environment variable is required", name)
	}
	return v
}

func main() {
	databaseURL := required("DATABASE_URL")
	natsURL := required("NATS_URL")
	s3Endpoint := required("S3_ENDPOINT")
	s3Bucket := required("S3_BUCKET")
	workflowsPath := os.Getenv("WORKFLOWS_FILE")
	if workflowsPath == "" {
		workflowsPath = "/app/config/workflows.yaml"
	}

	// Fail fast if the definitions are broken, rather than at the first run.
	set, err := LoadWorkflows(workflowsPath)
	if err != nil {
		log.Fatalf("load workflows: %v", err)
	}
	log.Printf("loaded %d workflow(s), default %q", len(set.Names()), set.Default())

	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		log.Fatalf("ping database: %v", err)
	}

	nc, err := nats.Connect(natsURL, nats.MaxReconnects(-1), nats.ReconnectWait(time.Second))
	if err != nil {
		log.Fatalf("connect nats %s: %v", natsURL, err)
	}
	defer nc.Close()

	st, err := storage.New(s3Endpoint, s3Bucket)
	if err != nil {
		log.Fatalf("object storage: %v", err)
	}
	if err := st.EnsureBucket(context.Background()); err != nil {
		log.Fatalf("object storage: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	executor := NewExecutor(db, nc, st)
	if err := executor.Start(ctx); err != nil {
		log.Fatalf("executor start: %v", err)
	}
	defer executor.Close()

	api := &API{executor: executor, workflowsPath: workflowsPath}
	srv := &http.Server{
		Addr:              ":5001",
		Handler:           api.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	// Shutdown returns once in-flight requests have finished. Wait for it, so
	// that the deferred closes do not pull the database and NATS from under them.
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdown); err != nil {
			log.Printf("http shutdown: %v", err)
		}
	}()

	log.Printf("executor ready on :5001")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("http: %v", err)
	}
	<-stopped
}
