package httpserver

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	proto2 "github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"io"
	"net/http"
	"strconv"
	"testConsumer/proto"
)

type Message struct {
	proto.Message
}

const (
	EStateAdddb string = "Add"
	EStateSubdb        = "Sub"
)

type Server struct {
	Server http.Server
}

type Handler struct {
	P *kafka.Producer
}

func NewServer(addr string) *Server {
	return &Server{Server: http.Server{Addr: addr}}
}
func (s Server) ServerStart(h Handler) error {
	http.HandleFunc("/add", h.addHttpAnswer)
	http.HandleFunc("/sub", h.subHttpAnswer)
	err := s.Server.ListenAndServe()
	if err != nil {
		logrus.WithFields(
			logrus.Fields{
				"package": "server",
				"func":    "ServerStart",
				"method":  "ListenAndServe",
			}).Fatalln(err)
	}
	return nil
}
func (s Server) ServerStop() error {
	return s.Server.Close()
}

func (h Handler) addHttpAnswer(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	money := r.URL.Query().Get("money")
	if id == "" || money == "" {
		io.WriteString(w, fmt.Sprintf("error"))
	} else {

		_, err := io.WriteString(w, fmt.Sprintf("id = %v, money = %v", id, money))

		if err != nil {
			logrus.WithFields(
				logrus.Fields{
					"package": "server",
					"func":    "addHttpAnswer",
					"method":  "WriteString",
				}).Warningln(err)
			return
		}

		//передам json
		var message Message
		id32, err := strconv.Atoi(id)
		if err != nil {
			logrus.WithFields(
				logrus.Fields{
					"package": "server",
					"func":    "addHttpAnswer",
					"method":  "Atoi",
				}).Warningln(err)
			return
		}
		message.Id = int32(id32)
		if money32, err := strconv.ParseFloat(money, 64); err == nil {
			message.Money = float32(money32)
		}
		if err != nil {
			logrus.WithFields(
				logrus.Fields{
					"package": "server",
					"func":    "addHttpAnswer",
					"method":  "ParseFloat",
				}).Warningln(err)
			return
		}

		msg, err := proto2.Marshal(&message)
		if err != nil {
			logrus.WithFields(
				logrus.Fields{
					"package": "server",
					"func":    "addHttpAnswer",
					"method":  "Marshal",
				}).Warningln(err)
			return
		}
		topic := EStateAdddb
		h.P.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: message.Id},
			Value:          []byte(msg),
			Key:            []byte(id),
		}, nil)
		h.P.Flush(15 * 1000)
	}

}
func (h Handler) subHttpAnswer(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	money := r.URL.Query().Get("money")
	if id == "" || money == "" {
		_, err := io.WriteString(w, fmt.Sprintf("error"))
		if err != nil {
			logrus.WithFields(
				logrus.Fields{
					"package": "server",
					"func":    "subHttpAnswer",
					"method":  "WriteString",
				}).Warningln(err)
			return
		}
	} else {

		_, err := io.WriteString(w, fmt.Sprintf("id = %v, money = %v", id, money))

		if err != nil {
			logrus.WithFields(
				logrus.Fields{
					"package": "server",
					"func":    "subHttpAnswer",
					"method":  "WriteString",
				}).Warningln(err)
			return
		}

		//передам json
		var message Message
		id32, err := strconv.Atoi(id)
		if err != nil {
			logrus.WithFields(
				logrus.Fields{
					"package": "server",
					"func":    "subHttpAnswer",
					"method":  "Atoi",
				}).Warningln(err)
			return
		}
		message.Id = int32(id32)
		if money32, err := strconv.ParseFloat(money, 64); err == nil {
			message.Money = float32(money32)
		}
		if err != nil {
			logrus.WithFields(
				logrus.Fields{
					"package": "server",
					"func":    "subHttpAnswer",
					"method":  "ParseFloat",
				}).Warningln(err)
			return
		}
		msg, err := proto2.Marshal(&message)
		if err != nil {
			logrus.WithFields(
				logrus.Fields{
					"package": "server",
					"func":    "subHttpAnswer",
					"method":  "Marshal",
				}).Warningln(err)
			return
		}
		topic := EStateSubdb
		h.P.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: message.Id},
			Value:          []byte(msg),
			Key:            []byte(id),
		}, nil)
		h.P.Flush(15 * 1000)
	}
}
