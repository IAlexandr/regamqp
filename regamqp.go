package regamqp

import (
	"github.com/streadway/amqp"
	"log"
	"strings"
)

var ch *amqp.Channel

func FailOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func AmqpConnect(url string) (*amqp.Connection, *amqp.Channel) {
	amqpUrl := strings.Join([]string{"amqp://", url, ":5672/"}, "");
	conn, err := amqp.Dial(amqpUrl)
	FailOnError(err, "Failed to connect to RabbitMQ")
	//defer conn.Close()
	ch, err = conn.Channel()
	FailOnError(err, "Failed to open a channel")
	//defer ch.Close()
	log.Printf("Channel created.")
	return conn, ch
}

func ConsumeByRegId(regId string, receiveFunc func(amqp.Delivery)) {
	q, err := ch.QueueDeclare(
		regId, // name
		false,   // durable
		false,   // delete when usused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	FailOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	FailOnError(err, "Failed to register a consumer")

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d)
			receiveFunc(d)
		}
	}()

	log.Printf(" [*] (%q) Waiting for messages. To exit press CTRL+C", regId)
}
