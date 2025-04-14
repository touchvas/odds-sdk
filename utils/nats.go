package utils

import (
	"encoding/json"
	"fmt"
	nats "github.com/nats-io/nats.go"
	"log"
	"os"
)

func PublishToNats(nc *nats.Conn, natsTopic string, payload interface{}) error {

	servicePrefix := os.Getenv("ODDS_SERVICE_QUEUE_PREFIX")
	payloadByte, _ := json.Marshal(payload)
	queueName := fmt.Sprintf("%s.%s", servicePrefix, natsTopic)

	err := nc.Publish(queueName, payloadByte)
	if err != nil {

		log.Printf("failed to publish to nats %s | err %s ", queueName, err.Error())
	}

	return err
}

// GetNatsConnection gets nats connection connection
func GetNatsConnection() *nats.Conn {

	natsURI := os.Getenv("ODDS_SERVICE_NATS_URI")
	log.Printf(" connecting to odds service nats %s ", natsURI)

	// if a CA is configured, update nats connect config with the path
	var natConnectOptions []nats.Option
	natsCA := os.Getenv("ODDS_SERVICE_NATS_CA")
	if _, err := os.Stat(natsCA); err == nil {

		natConnectOptions = append(natConnectOptions, nats.RootCAs(natsCA))
	}

	// nats.Connect just gives us a bare connection to NATS
	nc, err := nats.Connect(natsURI, natConnectOptions...)
	if err != nil {

		log.Printf(" err connecting to nats with url %s | %s ", natsURI, err.Error())

	}

	return nc

}
