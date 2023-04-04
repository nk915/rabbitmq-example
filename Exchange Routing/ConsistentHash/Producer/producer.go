package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	producer("exchange_consistent_hash")
}

func producer(exchangeName string) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		exchangeName,        // name
		"x-consistent-hash", // type
		false,               // durable
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		nil,                 // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := bodyFrom(os.Args)
	for i := 0; i < 1000; i++ {
		strIndex := strconv.Itoa(i)
		err = ch.PublishWithContext(ctx,
			exchangeName, // exchange
			strIndex,     // routing key
			false,        // mandatory
			false,        // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(strIndex + ":" + body),
			})
		failOnError(err, "Failed to publish a message")
		log.Printf(" Sent %s", body)
	}
}

func bodyFrom(args []string) string {
	var s string
	if len(args) < 2 {
		s = "hello"
	} else {
		s = strings.Join(args[1:], " ")
	}
	return s
}
