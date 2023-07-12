package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/caarlos0/env"
	"github.com/distuurbia/kafka/config"
	"github.com/segmentio/kafka-go"
)

func main() {
	var cfg config.Config
	if err := env.Parse(&cfg); err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}
	topic := cfg.Topic
	brokerAddress := cfg.BrokerAddress

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{brokerAddress},
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    1,
		BatchTimeout: 10 * time.Millisecond,
	})

	defer writer.Close()

	message := kafka.Message{
		Key:   []byte("key"),
		Value: []byte("wassaaap"),
	}
	for i := 0; i < 2000; i++ {
		err := writer.WriteMessages(context.Background(), message)
		if err != nil {
			log.Fatal("Failed to write message:", err)
		}
	}
	fmt.Println("Message sent successfully")
}
