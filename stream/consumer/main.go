package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	// Mongo
	connectionString := "mongodb://mongo:mongo@localhost:27017"

	// Connect to MongoDB
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(connectionString))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(context.Background())

	speedCol := client.Database("streams-db").Collection("speed")
	heartRateCol := client.Database("streams-db").Collection("heart_rate")

	//Kafka
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",
		"group.id":          "consumerGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}
	defer c.Close()

	c.SubscribeTopics([]string{"speed", "heart_rate"}, nil)

	// A signal handler or similar could be used to set this to false to break the loop.
	run := true

	for run {
		msg, err := c.ReadMessage(time.Second)
		if err == nil {
			switch *msg.TopicPartition.Topic {
			case "speed":

				document := map[string]interface{}{
					"time":  time.Now().String(),
					"speed": string(msg.Value),
				}

				_, err = speedCol.InsertOne(context.Background(), document)
				if err != nil {
					fmt.Println("Error writing to speed")
				}
			case "heart_rate":
				document := map[string]interface{}{
					"time":      time.Now().String(),
					"hear_rate": string(msg.Value),
				}

				_, err = heartRateCol.InsertOne(context.Background(), document)
				if err != nil {
					fmt.Println("Error writing to hear_rate")
				}
			}
		} else if !err.(kafka.Error).IsTimeout() {
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}
