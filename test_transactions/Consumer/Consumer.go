package Consumer

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	proto2 "github.com/golang/protobuf/proto"
	cache "github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"strconv"
	"testtransactions/database"
	"testtransactions/proto"
	"time"
)

type ForCache struct {
	Numtrans string
	Ok       bool
	Answer   string
}
type Consumer struct {
	P     *kafka.Consumer
	Cache *cache.Cache
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
	//c.Cache = cache.New(cache.DefaultExpiration, 0)
	go func() {
		for run {
			msg, err := c.P.ReadMessage(time.Millisecond)
			if err == nil {
				err = c.parser(msg)
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
func (c Consumer) parser(msg *kafka.Message) error {
	logrus.Infof("Message on %s: %s\n", msg.TopicPartition, string(msg.Key))
	var message proto.Message
	err := proto2.Unmarshal(msg.Value, &message)
	if err != nil {
		fmt.Println(err)
	}
	id := strconv.Itoa(int(message.Id))

	if message.Money < 0 {
		var msgCache ForCache
		msgCache.Answer = "Err: Изменение меньше 0\n"
		msgCache.Ok = true
		CacheEdit(c.Cache, msgCache, message.Numtrans, id)
		c.Cache.Set(message.Numtrans, msg, cache.DefaultExpiration)
		logrus.Infof("Изменение меньше 0; %s: %s :%v\n", msg.TopicPartition, string(msg.Key), message)

		return err
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
	switch *msg.TopicPartition.Topic {
	case EStateAdddb:

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
				var msgCache ForCache
				msgCache.Answer = fmt.Sprintf("Транзакция успешна \n ID %d добавлен в БД\n", message.Id)
				msgCache.Ok = true
				CacheEdit(c.Cache, msgCache, message.Numtrans, id)
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
			var msgCache ForCache
			msgCache.Answer = fmt.Sprintf("Транзакция успешна \nДля ID %d баланс изменен = %f\n", dbedit.IdUser, dbedit.Balance)
			msgCache.Ok = true
			CacheEdit(c.Cache, msgCache, message.Numtrans, id)
			c.Cache.Set(message.Numtrans, msg, cache.DefaultExpiration)
			logrus.Infof("Для ID %d баланс изменен = %f", dbedit.IdUser, dbedit.Balance)
		}

	case EStateSubdb:

		var dbedit database.T_DB
		err = db.Read(msg.TopicPartition.Partition, &dbedit)
		if err != nil {
			if err.Error() == "sql: no rows in result set" {
				var msgCache ForCache
				msgCache.Answer = "Транзакция не разрешена \nErr: ID не существует\n"
				msgCache.Ok = true
				CacheEdit(c.Cache, msgCache, message.Numtrans, id)
				c.Cache.Set(message.Numtrans, msg, cache.DefaultExpiration)
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
			NewBalance := dbedit.Balance - message.Money
			if NewBalance >= 0 {
				dbedit.Balance = NewBalance
				err = db.Edit(msg.TopicPartition.Partition, "balance", dbedit.Balance)
				if err != nil {
					logrus.WithFields(
						logrus.Fields{
							"package": "Consumer",
							"func":    "parser",
							"method":  "db.Edit",
						}).Fatalln(err)
				}
				var msgCache ForCache
				msgCache.Answer = fmt.Sprintf("Транзакция успешна \nДля ID %d баланс изменен = %f\n", dbedit.IdUser, dbedit.Balance)
				msgCache.Ok = true
				CacheEdit(c.Cache, msgCache, message.Numtrans, id)
				c.Cache.Set(message.Numtrans, msg, cache.DefaultExpiration)
				logrus.Infof("Для ID %d баланс изменен = %f", dbedit.IdUser, dbedit.Balance)
			} else {
				var msgCache ForCache
				msgCache.Answer = fmt.Sprintf("Транзакция не разрешена \nДля ID %d, ваш баланс= %f\n", dbedit.IdUser, dbedit.Balance)
				msgCache.Ok = true
				CacheEdit(c.Cache, msgCache, message.Numtrans, id)

				logrus.Infof("Транзакция невозможна, не хватает денет")
			}
		}
	}
	return nil
}
func CacheAdd(cache2 *cache.Cache, msgCahcheNew []ForCache, key string) {

	data, found := cache2.Get(key)
	if found {
		msgCache := data.([]ForCache)
		msgCache = append(msgCache, msgCahcheNew[0])
		cache2.Delete(key)
		cache2.Add(key, msgCache, cache.DefaultExpiration)
	} else {
		cache2.Add(key, msgCahcheNew, cache.DefaultExpiration)
	}
}
func CacheEdit(cache2 *cache.Cache, msgCahcheNew ForCache, Numtrans string, key string) {

	data, found := cache2.Get(key)
	if found {
		msgCache := data.([]ForCache)
		for i := range msgCache {
			if msgCache[i].Numtrans == Numtrans {
				msgCache[i].Answer = msgCahcheNew.Answer
				msgCache[i].Ok = msgCahcheNew.Ok
				//cache2.Delete(key)
				cache2.Set(key, msgCache, cache.DefaultExpiration)
			}
		}

	}
}
