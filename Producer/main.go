package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	proto2 "github.com/golang/protobuf/proto"
	"io"
	"net/http"
	"strconv"
	"testProducer/proto"
)

type Message struct {
	proto.Message
}
type Handler struct {
	p *kafka.Producer
}

func (h Handler) addHttpAnswer(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	money := r.URL.Query().Get("money")
	if id == "" || money == "" {
		io.WriteString(w, fmt.Sprintf("error"))
	} else {

		_, err := io.WriteString(w, fmt.Sprintf("id = %v, money = %v", id, money))

		if err != nil {
			fmt.Println(nil)
		}

		topic := "TutorialTopic"
		//передам json
		var message Message
		id32, err := strconv.Atoi(id)
		if err != nil {
			//
		}
		message.Id = int32(id32)
		if money32, err := strconv.ParseFloat(money, 64); err == nil {
			message.Money = float32(money32)
			fmt.Println(message.Money) // 3.14159265
		}

		fmt.Println(message)
		msg, err := proto2.Marshal(&message)

		h.p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(msg),
			Key:            []byte("1"),
		}, nil)
		h.p.Flush(15 * 1000)
	}

}
func (h Handler) subHttpAnswer(w http.ResponseWriter, r *http.Request) {
	id := r.RequestURI
	//s := r.URL.RawQuery
	_ = id
	_, err := io.WriteString(w, "aza no found\n")
	if err != nil {
		return
	}
	//}
}

//func (h Handler) takeHttpAnswer(w http.ResponseWriter, r *http.Request) {
//	_, err := io.WriteString(w, "aza no found\n")
//	if err != nil {
//		return
//	}
//}

// тут будет сервер hhttp
func main() {
	var (
		httpUrl = "127.0.0.1:3333"
	)
	var h Handler
	var err error
	h.p, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}

	defer h.p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range h.p.Events() {
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

	http.HandleFunc("/buy", h.addHttpAnswer)
	http.HandleFunc("/sub", h.subHttpAnswer)
	//http.HandleFunc("/take/", h.takeHttpAnswer)
	// Produce messages to topic (asynchronously)
	topic := "TutorialTopic"
	for _, word := range []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"} {
		h.p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(word),
			Key:            []byte("11"),
		}, nil)
	}

	// Wait for message deliveries before shutting down
	h.p.Flush(15 * 1000)
	err = http.ListenAndServe(httpUrl, nil)
	if err != nil {
		panic(err)
		//fmt.Printf(err.Error()) //to do
		//return
	}
}
