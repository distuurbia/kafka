package main

import (
	"context"
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)
func connectPostgres() (*pgxpool.Pool, error) {
	conf, err := pgxpool.ParseConfig("postgres://olegKafka:olegKafka@localhost:5432/kafkaDB")
	if err != nil {
		return nil, fmt.Errorf("error in method pgxpool.ParseConfig: %v", err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), conf)
	if err != nil {
		return nil, fmt.Errorf("error in method pgxpool.NewWithConfig: %v", err)
	}
	return pool, nil
}

func main() {
	topic := "my-topic"
	brokerAddress := "localhost:9092"
	groupID := "my-consumer-group"
	pool, err := connectPostgres()
	if err != nil {
		logrus.Fatalf("failed to connect postgres error: %v", err)
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		GroupID: groupID,
	})

	defer reader.Close()

	for {
		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal("Failed to read message:", err)
		}
		if message.Value != nil {
			id := uuid.New()
			pool.Exec(context.Background(), "INSERT INTO kafkamsg (id, key, message) VALUES ($1, $2, $3)", id, message.Key, message.Value)
		}
	}
}