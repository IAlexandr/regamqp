package regamqp

import (
	"github.com/streadway/amqp"
	"log"
	"strings"
)

/* v0.1.0 */

var CH *amqp.Channel

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
	CH, err = conn.Channel()
	FailOnError(err, "Failed to open a channel")
	//defer ch.Close()
	log.Println("Channel created.")
	return conn, CH
}

func ConsumeByRegId(regId string, receiveFunc func(amqp.Delivery)) {
	err := CH.ExchangeDeclare(
		"regs", // name
		"direct", // type
		true, // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil, // arguments
	)
	FailOnError(err, "Failed to declare an exchange")
	q, err := CH.QueueDeclare(
		"regs." + regId, // name
		false, // durable
		false, // delete when usused
		true, // exclusive
		false, // no-wait
		nil, // arguments
	)
	FailOnError(err, "Failed to declare a queue")
	err = CH.QueueBind(
		q.Name, // queue name
		"regs." + regId, // routing key
		"regs", // exchange
		false,
		nil)
	FailOnError(err, "Failed to bind a queue")
	msgs, err := CH.Consume(
		q.Name, // queue
		"", // consumer
		true, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil, // args
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

func PublishTo(exchangeName, route string, message []byte, Type string) error {
	err := CH.Publish(
		exchangeName, // exchange
		route, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        message,
			Type: Type,
		})
	log.Printf(" [x] Sent %s", string(message))
	return err
}
