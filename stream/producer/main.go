package main

import (
	"encoding/csv"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"os"
	"time"
)

// TODO
type Stats struct {
	Datetime  string
	Latitude  string
	Longitude string
	Altitude  string
	Distance  string
	HeartRate string
	Speed     string
}

func readCSV(path string) [][]string {
	// Open the CSV file
	file, err := os.Open(path)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return nil
	}
	defer file.Close()

	// Create a new CSV reader
	reader := csv.NewReader(file)
	reader.Comma = ';'

	// Read all records from the CSV file
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Println("Error reading CSV:", err)
		return nil
	}

	//without header
	return records[1:]
}

func main() {
	//Code from example: https://github.com/confluentinc/confluent-kafka-go
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094,"})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := "run_stats"

	statsCsv := readCSV("../../datasets/half_marathon_realtime.csv")

	for _, line := range statsCsv {
		time.Sleep(1 * time.Second)

		message := ""
		for _, word := range line {
			message += " " + word
		}

		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(message),
		}, nil)
	}

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)
}
