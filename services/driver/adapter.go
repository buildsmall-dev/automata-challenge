package main

import (
	"context"
	"log"

	"github.com/nats-io/nats.go"

	"github.com/automata-tech/distributed-systems-interview/services/common/storage"
	"github.com/automata-tech/distributed-systems-interview/services/driver/instrument"
)

// Serve is the driver side of the work. This file is yours.
//
// It connects one instrument to the rest of the system. It is called once when
// the driver process starts, and is expected to run until ctx ends. Keep the
// name and the signature.
//
// The instrument is in instrument/instrument.go. Read it, but do not change it.
func Serve(ctx context.Context, inst *instrument.Instrument, nc *nats.Conn, st *storage.Storage) error {
	// TODO
	log.Printf("driver %s: nothing connects this instrument to anything yet", inst.DeviceID())
	<-ctx.Done()
	return ctx.Err()
}
