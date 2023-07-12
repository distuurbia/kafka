package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	topic := "my-topic"
	brokerAddress := "localhost:9092"

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{brokerAddress},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
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