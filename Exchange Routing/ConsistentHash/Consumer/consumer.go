package main

import (
	"fmt"
	"log"
	"strconv"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	consumer("exchange_consistent_hash")
}

func consumer(exchangeName string) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	weight, err := getRoutingKey()
	failOnError(err, "Failed to consistent-hash key wrong.")

	err = ch.ExchangeDeclare(
		exchangeName,        // name
		"x-consistent-hash", // type
		false,               // durable
		true,                // auto-deleted
		false,               // internal
		false,               // no-wait
		nil,                 // arguments
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
	ch.QueuePurge(q.Name, false)

	err = ch.QueueBind(
		q.Name,       // queue name
		weight,       // routing key : consistent-hash는 라우팅키를 숫자로 표현해야함!
		exchangeName, // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue")

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
		count := 0
		for d := range msgs {
			count = count + 1
			log.Printf(" [%s] %s (count: %d)", q.Name, d.Body, count)
		}
	}()

	log.Printf(" [%s:%s] Waiting for logs. To exit press CTRL+C", q.Name, weight)
	<-forever
}

func getRoutingKey() (string, error) {
	var weight string
	fmt.Print("weight:")
	fmt.Scanln(&weight)

	if _, err := strconv.Atoi(weight); err != nil {
		return "", err
	}

	return weight, nil
}
