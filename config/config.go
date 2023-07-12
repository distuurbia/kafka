// Package config represents struct Config.
package config

// Config is a structure of environment variables.
type Config struct {
	PostgresPathKafka string `env:"POSTGRES_PATH_KAFKA"`
	BrokerAddress string `env:"BROKER_ADDRESS"`
	GroupID string `env:"GROUP_ID"`
	Topic string `env:"TOPIC"`
}
