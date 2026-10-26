// Starts one driver: the instrument, its status side channel, and your adapter.
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/automata-tech/distributed-systems-interview/services/common/storage"
	"github.com/automata-tech/distributed-systems-interview/services/driver/instrument"
)

func required(name string) string {
	v := os.Getenv(name)
	if v == "" {
		log.Fatalf("%s environment variable is required", name)
	}
	return v
}

func duration(name, def string) time.Duration {
	raw := os.Getenv(name)
	if raw == "" {
		raw = def
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 {
		log.Fatalf("%s is not a duration: %q", name, raw)
	}
	return d
}

func pct(name string) int {
	raw := os.Getenv(name)
	if raw == "" {
		return 0
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 0 || v > 100 {
		log.Fatalf("%s must be a whole number from 0 to 100, got %q", name, raw)
	}
	return v
}

func flag(name string, def bool) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "":
		return def
	case "true", "1", "yes":
		return true
	case "false", "0", "no":
		return false
	default:
		log.Fatalf("%s must be true or false, got %q", name, os.Getenv(name))
		return def
	}
}

func main() {
	deviceID := required("WORKER_ID")
	natsURL := required("NATS_URL")
	st, err := storage.New(required("S3_ENDPOINT"), required("S3_BUCKET"))
	if err != nil {
		log.Fatalf("object storage: %v", err)
	}

	failPct, hangPct := pct("FAIL_PCT"), pct("HANG_PCT")
	if failPct+hangPct > 100 {
		log.Fatalf("FAIL_PCT + HANG_PCT must not be more than 100, got %d", failPct+hangPct)
	}
	ledgerDir := os.Getenv("LEDGER_DIR")
	if ledgerDir == "" {
		ledgerDir = "/ledger"
	}
	seed := os.Getenv("CHAOS_SEED")
	if seed == "" {
		seed = "0"
	}

	inst, err := instrument.New(instrument.Config{
		DeviceID:          deviceID,
		Duration:          duration("STEP_DURATION", "2s"),
		FailPct:           failPct,
		HangPct:           hangPct,
		HangDuration:      duration("HANG_DURATION", "30s"),
		FailuresRetryable: flag("FAILURES_RETRYABLE", true),
		ProducesData:      flag("PRODUCES_DATA", false),
		Seed:              seed,
		LedgerDir:         ledgerDir,
	})
	if err != nil {
		log.Fatalf("instrument: %v", err)
	}
	instrument.ServeStatus(inst, ":7000")
	log.Printf("instrument %s ready (step %s, fail %d%%, hang %d%%)", deviceID, inst.Duration(), failPct, hangPct)

	nc, err := nats.Connect(natsURL, nats.MaxReconnects(-1), nats.ReconnectWait(time.Second),
		// The default (2 min x 2) let a 20s partition go unnoticed (chaos-log C1).
		nats.PingInterval(5*time.Second), nats.MaxPingsOutstanding(2))
	if err != nil {
		log.Fatalf("connect nats %s: %v", natsURL, err)
	}
	defer nc.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	err = Serve(ctx, inst, nc, st)
	switch {
	case err != nil && !errors.Is(err, context.Canceled):
		log.Fatalf("driver %s: Serve stopped with an error: %v", deviceID, err)
	case ctx.Err() == nil:
		log.Fatalf("driver %s: Serve returned; it is expected to run until shutdown", deviceID)
	}
	log.Printf("driver %s shutting down", deviceID)
}
