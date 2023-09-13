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
	consumer()
}

func consumer() {
	queueName := getQueueName()
	routingKeys := getRoutingKey()
	exchangeName := "exchange_direct"

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		exchangeName, // name
		"direct",     // type
		false,        // durable
		true,         // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		queueName, // name
		false,     // durable
		true,      // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	if len(routingKeys) < 1 {
		log.Printf("not found routing-key", routingKeys)
		os.Exit(0)
	}
	for _, key := range routingKeys {
		log.Printf("Binding queue %s to exchange %s with routing key %s", q.Name, exchangeName, key)
		err = ch.QueueBind(
			q.Name,       // queue name
			key,          // routing key
			exchangeName, // exchange
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

	log.Printf(" (%s -> %v) Waiting for logs. To exit press CTRL+C", queueName, routingKeys)
	<-forever
}

func getQueueName() string {
	var QueueName string
	fmt.Print("QueueName:")
	fmt.Scanln(&QueueName)
	return QueueName
}

func getRoutingKey() []string {
	//var routingKeys []string

	var routingKeyCount int
	fmt.Print("RoutingKey Count:")
	fmt.Scanln(&routingKeyCount)

	routingKeys := make([]string, routingKeyCount)
	for i := 0; i < routingKeyCount; i++ {
		fmt.Printf("[%d] RoutingKey:", i)
		fmt.Scanln(&routingKeys[i])
	}

	return routingKeys
}
