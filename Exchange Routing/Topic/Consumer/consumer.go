package main

import (
	"fmt"
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	consumer("exchange_topic")
}

func consumer(topicName string) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	keys := getTopicKeys()
	if len(keys) < 1 {
		//log.Printf("Usage: %s [binding_key]...", os.Args[0])
		log.Printf("not found: %v...", keys)
		os.Exit(0)
	}

	err = ch.ExchangeDeclare(
		topicName, // name
		"topic",   // type
		false,     // durable
		true,      // auto-deleted
		false,     // internal
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		true,  // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	for _, key := range keys {
		log.Printf("Binding queue %s to exchange %s with routing key %s", q.Name, topicName, key)
		err = ch.QueueBind(
			q.Name,    // queue name
			key,       // routing key
			topicName, // exchange
			false,
			nil)
		failOnError(err, "Failed to bind a queue")
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		for d := range msgs {
			log.Printf(" [%s] %s", q.Name, d.Body)
		}
	}()

	log.Printf(" (%s -> %v) Waiting for logs. To exit press CTRL+C", q.Name, keys)
	<-forever
}

func getTopicKeys() []string {
	var keyCount int
	fmt.Print("TopicKey Count:")
	fmt.Scanln(&keyCount)

	keys := make([]string, keyCount)
	for i := 0; i < keyCount; i++ {
		fmt.Printf("[%d] TopicKey:", i)
		fmt.Scanln(&keys[i])
	}

	return keys
}
