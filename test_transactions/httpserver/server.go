package httpserver

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	proto2 "github.com/golang/protobuf/proto"
	cache "github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"testtransactions/Consumer"
	"testtransactions/Producer"
	"testtransactions/proto"
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
	P     *kafka.Producer
	Cache *cache.Cache
	//C *kafka.Consumer
}

func NewServer(addr string) *Server {
	return &Server{Server: http.Server{Addr: addr}}
}
func (s Server) ServerStart(Cache *cache.Cache) error {
	//consumer
	var h Handler
	c := Consumer.NewConsumer()
	c.Cache = Cache
	h.Cache = Cache
	//h.C = c.P
	c.ConsumerStart()
	defer c.P.Close()

	//producer
	prod := Producer.NewProducer()
	defer prod.P.Close()

	h.P = prod.P
	http.HandleFunc("/add", h.addHttpAnswer)
	http.HandleFunc("/sub", h.subHttpAnswer)
	http.HandleFunc("/check", h.checkHttpAnswer)
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
		var message Message
		message.Numtrans = strconv.Itoa(rand.Int())
		msgCacheNew := make([]Consumer.ForCache, 1)
		msgCacheNew[0].Numtrans = message.Numtrans
		msgCacheNew[0].Ok = false
		Consumer.CacheAdd(h.Cache, msgCacheNew, id)
		_, err := io.WriteString(w, fmt.Sprintf("id = %v, money = %v\nНомер транзакции: %s", id, money, message.Numtrans))

		if err != nil {
			logrus.WithFields(
				logrus.Fields{
					"package": "server",
					"func":    "addHttpAnswer",
					"method":  "WriteString",
				}).Warningln(err)
			return
		}

		go func() {
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
		}()
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
		var message Message
		message.Numtrans = strconv.Itoa(rand.Int())
		msgCacheNew := make([]Consumer.ForCache, 1)
		msgCacheNew[0].Numtrans = message.Numtrans
		msgCacheNew[0].Ok = false
		Consumer.CacheAdd(h.Cache, msgCacheNew, id)
		_, err := io.WriteString(w, fmt.Sprintf("id = %v, money = %v\nНомер транзакции: %s", id, money, message.Numtrans))

		if err != nil {
			logrus.WithFields(
				logrus.Fields{
					"package": "server",
					"func":    "subHttpAnswer",
					"method":  "WriteString",
				}).Warningln(err)
			return
		}
		go func() {
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
		}()
	}
}
func (h Handler) checkHttpAnswer(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	data, found := h.Cache.Get(id)
	str := ""
	if found {
		msgCache := data.([]Consumer.ForCache)
		for i := range msgCache {
			if msgCache[i].Ok == true {
				str += fmt.Sprintf("Номер транзакции: %s\nСтатус: %v\nОтвет: %s\n\n", msgCache[i].Numtrans, msgCache[i].Ok, msgCache[i].Answer)
			} else {
				str += fmt.Sprintf("Номер транзакции: %s\nСтатус: В обработке\n\n", msgCache[i].Numtrans)
			}

		}
	} else {
		str = "Транзакции не найдены"
	}
	_, err := io.WriteString(w, str)
	if err != nil {
		logrus.WithFields(
			logrus.Fields{
				"package": "server",
				"func":    "subHttpAnswer",
				"method":  "WriteString",
			}).Warningln(err)
		return
	}

}
