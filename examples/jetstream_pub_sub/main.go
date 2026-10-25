// JetStream: publish to a stream, read it back through a durable consumer.
//
//	go run ./examples/jetstream_pub_sub
//
// Runs both sides in one process against the stack's NATS server. Nothing here
// is part of the system; it only shows the client API.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	streamName = "EXAMPLES"
	subject    = "examples.readings"
)

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

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// A stream stores every message published to its subjects.
	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{"examples.>"},
	})
	if err != nil {
		log.Fatalf("create stream: %v", err)
	}

	// Publishing to a stream returns once the server has stored the message.
	for _, value := range []string{"0.41", "0.52", "0.38"} {
		ack, err := js.Publish(ctx, subject, []byte(value))
		if err != nil {
			log.Fatalf("publish: %v", err)
		}
		fmt.Printf("stored %s as message %d\n", value, ack.Sequence)
	}

	// A durable consumer remembers what it has been given, across restarts.
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   "example-reader",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		log.Fatalf("create consumer: %v", err)
	}
	batch, err := cons.Fetch(10, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		log.Fatalf("fetch: %v", err)
	}
	for msg := range batch.Messages() {
		fmt.Println("read:", string(msg.Data()))
		if err := msg.Ack(); err != nil {
			log.Fatalf("ack: %v", err)
		}
	}
	if err := batch.Error(); err != nil {
		log.Fatalf("fetch: %v", err)
	}

	// Clean up, so running it again starts from nothing.
	if err := js.DeleteStream(ctx, streamName); err != nil {
		log.Fatalf("delete stream: %v", err)
	}
}
