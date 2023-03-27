package main

import (
	"github.com/ghodss/yaml"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"os"
	"testtransactions/database"
	"testtransactions/httpserver"
	"testtransactions/proto"
)

type Message struct {
	proto.Message
}
type ForCache struct {
	Numtrans string
	Ok       bool
	Answer   string
}

func main() {
	type cfg struct {
		Port      string `yaml:"port"`
		TableName string `yaml:"tableName"`
		Addr      string `yaml:"addr"`
	}
	ucfg := cfg{}
	data, err := os.ReadFile("conf/conf.yml")
	if err != nil {
		logrus.WithFields(
			logrus.Fields{
				"package": "main",
				"func":    "main",
				"method":  "ReadFile",
			}).Fatalf("err read config: %v", err)
	}
	err = yaml.Unmarshal([]byte(data), &ucfg)
	if err != nil {
		logrus.WithFields(
			logrus.Fields{
				"package": "main",
				"func":    "main",
				"method":  "Unmarshal",
			}).Fatalf("err unmarshal config: %v", err)
	}
	if ucfg.Addr != "" {
		logrus.Infof("addr: %v", ucfg.Addr)
	} else {
		logrus.WithFields(
			logrus.Fields{
				"package": "main",
				"func":    "main",
			}).Fatal("err config: addr")
	}
	if ucfg.Port != "" {
		logrus.Infof("port: %v", ucfg.Port)
	} else {
		logrus.WithFields(
			logrus.Fields{
				"package": "main",
				"func":    "main",
			}).Fatal("err config: port")
	}
	if ucfg.TableName != "" {
		logrus.Infof("tableName: %v", ucfg.TableName)
	} else {
		logrus.WithFields(
			logrus.Fields{
				"package": "main",
				"func":    "main",
			}).Fatal("err config: tablename")
	}
	//httpUrl :=
	err = database.Create(ucfg.TableName)
	if err != nil {
		logrus.WithFields(
			logrus.Fields{
				"package": "main",
				"func":    "main",
				"method":  "database.Create(tableName)",
			}).Fatalln(err)
	}
	//cache
	c := cache.New(cache.DefaultExpiration, 0)
	//http
	httpUrl := ucfg.Addr + ":" + ucfg.Port
	httpserver.NewServer(httpUrl).ServerStart(c)
	if err != nil {
		logrus.WithFields(
			logrus.Fields{
				"package": "main",
				"func":    "main",
				"method":  "ServerStart",
			}).Fatalln(err)
	}
}
