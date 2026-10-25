// Core NATS: one side answers requests, the other asks.
//
//	go run ./examples/core_request_reply
//
// Runs both sides in one process against the stack's NATS server. Nothing here
// is part of the system; it only shows the client API.
package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

const subject = "examples.greet"

func main() {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = "nats://localhost:4242"
	}
	nc, err := nats.Connect(url)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	// The answering side: every message on the subject gets a reply.
	sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		if err := msg.Respond([]byte("hello, " + string(msg.Data))); err != nil {
			log.Printf("respond: %v", err)
		}
	})
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	// The asking side: send a request and wait for one reply.
	reply, err := nc.Request(subject, []byte("plate reader"), 2*time.Second)
	if err != nil {
		log.Fatalf("request: %v", err)
	}
	fmt.Println("reply:", string(reply.Data))

	// With nobody subscribed, a request fails straight away.
	if err := sub.Unsubscribe(); err != nil {
		log.Fatalf("unsubscribe: %v", err)
	}
	if _, err := nc.Request(subject, []byte("anyone?"), 2*time.Second); errors.Is(err, nats.ErrNoResponders) {
		fmt.Println("no responders")
	} else if err != nil {
		log.Fatalf("request: %v", err)
	}
}
