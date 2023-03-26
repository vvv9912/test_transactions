package main

import (
	"github.com/sirupsen/logrus"
	"testtransactions/Consumer"
	"testtransactions/Producer"
	"testtransactions/database"
	"testtransactions/httpserver"
	"testtransactions/proto"
)

type Message struct {
	proto.Message
}

func main() {
	var (
		tableName = "test"
		httpUrl   = "127.0.0.1:3333"
	)
	err := database.Create(tableName)

	if err != nil {
		logrus.WithFields(
			logrus.Fields{
				"package": "main",
				"func":    "main",
				"method":  "database.Create(tableName)",
			}).Fatalln(err)
	}

	//consumer
	c := Consumer.NewConsumer()
	c.ConsumerStart()
	defer c.C.Close()
	//producer
	prod := Producer.NewProducer()

	defer prod.P.Close()
	//http
	//err = httpserver.NewServer(httpUrl).ServerStart(httpserver.Handler{P: prod.P})
	httpserver.NewServer(httpUrl).ServerStart(httpserver.Handler{P: prod.P})
	if err != nil {
		logrus.WithFields(
			logrus.Fields{
				"package": "main",
				"func":    "main",
				"method":  "ServerStart",
			}).Fatalln(err)
	}
}
