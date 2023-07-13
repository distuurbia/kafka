package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/caarlos0/env"
	"github.com/distuurbia/kafka/config"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)
type dbStruct struct{
	message kafka.Message
	id uuid.UUID
}

func connectPostgres(cfg *config.Config) (*pgxpool.Pool, error) {
	conf, err := pgxpool.ParseConfig(cfg.PostgresPathKafka)
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
	var cfg config.Config
	if err := env.Parse(&cfg); err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}
	topic := cfg.Topic
	brokerAddress := cfg.BrokerAddress
	groupID := cfg.GroupID
	pool, err := connectPostgres(&cfg)
	if err != nil {
		logrus.Fatalf("failed to connect postgres error: %v", err)
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		GroupID: groupID,
	})

	defer reader.Close()
	msgCount := 0

	var messages []*dbStruct
	start := time.Now()
	
	for time.Since(start) < time.Second && msgCount < 4000{ //&& msgCount < 4000
		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			logrus.Fatalf("failed to read message: %v", err)
		}
		if message.Value != nil {
			id := uuid.New()
			messages = append(messages, &dbStruct{message: message, id: id})
			msgCount++
		}
	}


	conn, err := pool.Acquire(context.Background())
	if err != nil {
		logrus.Fatalf("failed to acquire connection from pool: %v", err)
	}
	writeToPostgres(conn, messages)

	fmt.Println(msgCount, " Message read successfully")
}

func writeToPostgres(conn *pgxpool.Conn, messages []*dbStruct) {
	batch := &pgx.Batch{}
	for _, msg := range messages{
		batch.Queue("INSERT INTO kafkamsg (id, key, message) VALUES ($1, $2, $3)", msg.id, msg.message.Key, msg.message.Value)
	}

	br := conn.SendBatch(context.Background(), batch)
	for i := 0; i < len(messages); i++{
		_, err := br.Exec()
		if err != nil {
			logrus.Fatalf("failed to exec batch: %v", err)
		}
	}
	defer br.Close()
	defer conn.Release()
}