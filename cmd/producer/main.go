package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {
	deliveryChannel := make(chan kafka.Event)
	producer := NewKafkaProducer()
	Publish("Hello World", "test", producer, []byte("transferencia"), deliveryChannel)
	go DeliveryReport(deliveryChannel)
	//e := <-deliveryChannel
	//msg := e.(*kafka.Message)
	//if msg.TopicPartition.Error != nil {
	//	fmt.Println("Erro ao enviar")
	//} else {
	//	fmt.Println("Mensagem enviada", msg.TopicPartition)
	//}
	producer.Flush(10000)
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":   "fullcycle-3-messaging-kafka-go_kafka_1:9092",
		"delivery.timeout.ms": "0",
		"acks":                "all",
		"enable.idempotence":  "true",
	}
	p, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err.Error())
	}
	return p
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChannel chan kafka.Event) error {
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(msg),
		Key:            key,
	}
	err := producer.Produce(message, deliveryChannel)
	if err != nil {
		return err
	}
	return nil
}

func DeliveryReport(deliveryChannel chan kafka.Event) {
	for e := range deliveryChannel {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Printf("Delivery failed: %v", ev.TopicPartition)
			} else {
				fmt.Printf("Delivered message to %v", ev.TopicPartition)
			}
		}
	}
}
