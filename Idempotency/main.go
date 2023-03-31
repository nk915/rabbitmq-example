package main

import (
	"encoding/json"
	"log"

	"github.com/go-redis/redis"
	"github.com/streadway/amqp"
)

// Use Redis to store message IDs
var redisClient = redis.NewClient(&redis.Options{
	Addr: "localhost:6379",
})

// Define function to check if a message has been processed
func isMessageProcessed(messageID string) bool {
	result, err := redisClient.Get(messageID).Result()
	if err != nil {
		return false
	}
	return result == "processed"
}

// Define function to process messages
func processMessage(d amqp.Delivery) {
	// Check if message has already been processed
	messageID := d.CorrelationId
	if isMessageProcessed(messageID) {
		log.Printf("Message %s has already been processed\n", messageID)
		d.Ack(false)
		return
	}

	// Deserialize message payload
	var message Message
	err := json.Unmarshal(d.Body, &message)
	if err != nil {
		log.Printf("Error deserializing message: %s\n", err)
		d.Nack(false, true)
		return
	}

	// Process message
	err = doSomething(message)
	if err != nil {
		log.Printf("Error processing message: %s\n", err)
		d.Nack(false, true)
		return
	}

	// Mark message as processed in Redis
	err = redisClient.Set(messageID, "processed", 0).Err()
	if err != nil {
		log.Printf("Error marking message as processed: %s\n", err)
		d.Nack(false, true)
		return
	}

	// Acknowledge message
	d.Ack(false)
}

func main() {
	// Connect to RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Error connecting to RabbitMQ: %s", err)
	}
	defer conn.Close()

	// Create channel and queue
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Error creating channel: %s", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"test_queue", // queue name
		false,        // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		log.Fatalf("Error declaring queue: %s", err)
	}

	// Consume messages from queue
	msgs, err := ch.Consume(
		q.Name, // queue name
		"",     // consumer name
		false,  // auto-acknowledge
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // arguments
	)
	if err != nil {
		log.Fatalf("Error consuming messages: %s", err)
	}

	// Process messages
	for d := range msgs {
		go processMessage(d)
	}
}
