package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"

	"github.com/segmentio/kafka-go"
)

func main() {
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		topic = "booking-events"
	}
	group := os.Getenv("KAFKA_GROUP")
	if group == "" {
		group = "booking-event-log"
	}
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: group, MinBytes: 1, MaxBytes: 10e6})
	defer reader.Close()
	log.Printf("listening for %s", topic)
	for {
		m, e := reader.ReadMessage(context.Background())
		if e != nil {
			log.Printf("read error: %v", e)
			continue
		}
		var event map[string]any
		if e = json.Unmarshal(m.Value, &event); e != nil {
			log.Printf("invalid event: %s", m.Value)
			continue
		}
		// Replace this log handler with notification/analytics consumers as the next learning step.
		log.Printf("booking event received: key=%s payload=%v", string(m.Key), event)
	}
}
