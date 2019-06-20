package main

import (
	prop "github.com/magiconair/properties"
	log "github.com/sirupsen/logrus"
	"msq.ai/data"
	"msq.ai/feed/test/generator"
	"msq.ai/helper/config"
	"os"
)

const propFileName string = "feed-publisher.properties"
const bufferSize int16 = 1000 // TODO !!!

var defaultProperties = map[string]string{"key1": "value1",
	"key2": "value2"}

func init() {

	log.SetFormatter(&log.TextFormatter{
		DisableColors: true,
		FullTimestamp: true,
	})

	log.SetOutput(os.Stdout)

	log.SetLevel(log.TraceLevel)
}

func main() {

	log.Info("feed-publisher is starting")

	pwd, _ := os.Getwd()

	log.Debug("Current folder is [" + pwd + "]")

	properties := config.LoadProperties(propFileName, prop.LoadMap(defaultProperties))

	if properties == nil {
		log.Fatal("Properties has been set !!!")
	}

	quotes := make(chan *data.Quote, bufferSize)
	signals := make(chan bool)

	generator.MakeFeedGenerator(properties, quotes, signals)()

	signals <- true

	for {

		quote := <-quotes

		log.Info("quote [" + quote.Instrument + "]")
	}

	//time.Sleep(1 * time.Second)
}
