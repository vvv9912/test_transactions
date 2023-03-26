package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	proto2 "github.com/golang/protobuf/proto"
	"strconv"
	"testConsumer/database"
	"testConsumer/proto"
	"time"
)

// echo "Hello World from Sammy at DigitalOcean!" | ~/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic TutorialTopic > /dev/null
func main() {
	var (
		bootstrapservers = "localhost"
		groupid          = "myGroup"
		autooffsetreset  = "earliest"
		tableName        = "test"
	)
	const (
		EStateAdddb      int = 0
		EStateGetbalance     = 1
	)

	//http

	//kafka
	err := database.Create(tableName)
	if err != nil {
		fmt.Println(err)
	}
	//db, err := database.NewDB().Open()
	//if err != nil {
	//	fmt.Println(err)
	//}
	//db.Table_name = "test"
	//err = db.Add(database.T_DB{Balance: 1111, IdUser: 1})
	//if err != nil {
	//	fmt.Println(err)
	//}
	//var check database.T_DB
	//err = db.Read(1, &check)
	//if err != nil {
	//	fmt.Println(err)
	//}
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapservers,
		"group.id":          groupid,
		"auto.offset.reset": autooffsetreset,
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{"TutorialTopic"}, nil)

	// A signal handler or similar could be used to set this to false to break the loop.
	run := true
	//go func() {
	fmt.Printf("hi")
	for run {
		msg, err := c.ReadMessage(time.Millisecond)
		if err == nil {
			if len(msg.Key) != 0 {
				key, err := strconv.Atoi(string(msg.Key))
				if err != nil {
					fmt.Println(err)
				} else {
					switch key {
					case EStateAdddb:
						var adddb database.T_DB
						err = json.Unmarshal(msg.Value, &adddb)
						if err != nil {
							fmt.Println(err)
						}

					case EStateGetbalance:
						var a proto.Message
						err = proto2.Unmarshal(msg.Value, &a)
						if err != nil {
							fmt.Println(err)
						}
						fmt.Println(a)
					}
				}
				fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			}
		} else if !err.(kafka.Error).IsTimeout() {
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
	//}()

	c.Close()
}
