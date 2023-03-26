package Consumer

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	proto2 "github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"testtransactions/database"
	"testtransactions/proto"
	"time"
)

type Consumer struct {
	P *kafka.Consumer
}

var (
	bootstrapservers = "localhost"
	groupid          = "myGroup"
	autooffsetreset  = "earliest"
)

const (
	EStateAdddb string = "Add"
	EStateSubdb        = "Sub"
)

func NewConsumer() Consumer {
	//&
	kfk, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapservers,
		"group.id":          groupid,
		"auto.offset.reset": autooffsetreset,
	})
	if err != nil {
		logrus.WithFields(
			logrus.Fields{
				"package": "Consumer",
				"func":    "NewConsumer",
				"method":  "NewConsumer",
			}).Fatalln(err)
	}
	return Consumer{P: kfk}
}

func (c Consumer) ConsumerStart() error {
	c.P.SubscribeTopics([]string{"Add", "Sub"}, nil)
	// A signal handler or similar could be used to set this to false to break the loop.
	run := true
	go func() {
		for run {
			msg, err := c.P.ReadMessage(time.Millisecond)
			if err == nil {
				err = parser(msg)
				if err != nil {
					logrus.WithFields(
						logrus.Fields{
							"package": "Consumer",
							"func":    "ConsumerStart",
							"method":  "parser",
						}).Warningln(err)
				}
			} else if !err.(kafka.Error).IsTimeout() {
				// The client will automatically try to recover from all errors.
				// Timeout is not considered an error because it is raised by
				// ReadMessage in absence of messages.
				logrus.WithFields(
					logrus.Fields{
						"package": "Consumer",
						"func":    "ConsumerStart",
						"method":  "!err.(kafka.Error).IsTimeout()",
					}).Warningln(err)
			}
		}
	}()
	return nil
}

// todo
// Вычитание/суммирование лучше вынести в бд запросы
// тк сейчас parser(msg) запускается не в горутине, гонки нет.
func parser(msg *kafka.Message) error {
	logrus.Infof("Message on %s: %s\n", msg.TopicPartition, string(msg.Key))
	switch *msg.TopicPartition.Topic {
	case EStateAdddb:
		var message proto.Message
		err := proto2.Unmarshal(msg.Value, &message)
		if err != nil {
			fmt.Println(err)
		}
		if message.Money < 0 {
			logrus.Infof("Изменение меньше 0; %s: %s :%v\n", msg.TopicPartition, string(msg.Key), message)
			break
		}
		db, err := database.NewDB().Open()
		if err != nil {
			logrus.WithFields(
				logrus.Fields{
					"package": "Consumer",
					"func":    "parser",
					"method":  "database.NewDB().Open()",
				}).Fatalln(err)
		}
		db.Table_name = "test"
		var dbedit database.T_DB
		err = db.Read(msg.TopicPartition.Partition, &dbedit)
		if err != nil {
			if err.Error() == "sql: no rows in result set" {
				err = nil
				err = db.Add(database.T_DB{IdUser: msg.TopicPartition.Partition, Balance: message.Money})
				if err != nil {
					logrus.WithFields(
						logrus.Fields{
							"package": "Consumer",
							"func":    "parser",
							"method":  "db.Add",
						}).Fatalln(err)
				}
				logrus.Infof("ID %d добавлен в БД", message.Id)
			} else {
				logrus.WithFields(
					logrus.Fields{
						"package": "Consumer",
						"func":    "parser",
						"method":  "db.Read",
					}).Fatalln(err)
			}
		} else {
			dbedit.Balance += message.Money
			err = db.Edit(msg.TopicPartition.Partition, "balance", dbedit.Balance)
			if err != nil {
				logrus.WithFields(
					logrus.Fields{
						"package": "Consumer",
						"func":    "parser",
						"method":  "db.Edit",
					}).Fatalln(err)
			}
			logrus.Infof("Для ID %d баланс изменен = %f", dbedit.IdUser, dbedit.Balance)
		}

	case EStateSubdb:
		var message proto.Message

		err := proto2.Unmarshal(msg.Value, &message)
		if err != nil {
			fmt.Println(err)
		}
		if message.Money < 0 {
			logrus.Infof("Изменение меньше 0; %s: %s :%v\n", msg.TopicPartition, string(msg.Key), message)
			break
		}
		db, err := database.NewDB().Open()
		if err != nil {
			logrus.WithFields(
				logrus.Fields{
					"package": "Consumer",
					"func":    "parser",
					"method":  "database.NewDB().Open()",
				}).Fatalln(err)
		}
		db.Table_name = "test"
		var dbedit database.T_DB
		err = db.Read(msg.TopicPartition.Partition, &dbedit)
		if err != nil {
			if err.Error() == "sql: no rows in result set" {
				logrus.Infof("ID не существует")
				break
			} else {
				logrus.WithFields(
					logrus.Fields{
						"package": "Consumer",
						"func":    "parser",
						"method":  "db.Read",
					}).Fatalln(err)
			}
		} else {
			dbedit.Balance = dbedit.Balance - message.Money
			if dbedit.Balance > 0 {
				err = db.Edit(msg.TopicPartition.Partition, "balance", dbedit.Balance)
				if err != nil {
					logrus.WithFields(
						logrus.Fields{
							"package": "Consumer",
							"func":    "parser",
							"method":  "db.Edit",
						}).Fatalln(err)
				}
				logrus.Infof("Для ID %d баланс изменен = %f", dbedit.IdUser, dbedit.Balance)
			} else {
				logrus.Infof("Транзакция невозможна, не хватает денет")
			}
		}
	}
	return nil
}
