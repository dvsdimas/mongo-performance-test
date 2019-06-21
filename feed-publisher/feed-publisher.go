package main

import (
	prop "github.com/magiconair/properties"
	log "github.com/sirupsen/logrus"
	"msq.ai/data"
	"msq.ai/db/mongo"
	"msq.ai/feed/test/generator"
	"msq.ai/helper/config"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

const propFileName string = "feed-publisher.properties"
const bufferSize int16 = 10000 // TODO !!!

var defaultProperties = map[string]string{"key1": "value1", "key2": "value2"}

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

	quotesIn := make(chan *data.Quote, bufferSize)
	quotesOut := make(chan *data.Quote, bufferSize)
	signals := make(chan bool)

	send := func(quote *data.Quote) {
		select {
		case quotesOut <- quote:
		default:
			log.Fatal("out chanel buffer is overflowed !!!")
		}
	}

	generator.MakeFeedGenerator(properties, quotesIn, signals)()
	mongo.MakeMongoConnector(properties, quotesOut, signals)()

	var counter uint64 = 0

	go func() {

		var prev uint64 = 0

		for {

			var delta uint64

			delta, prev = counter-prev, counter

			log.Info("Producing [" + strconv.FormatUint(delta, 10) + "] quotes per second ")

			time.Sleep(1 * time.Second)
		}
	}()

	for {

		quote := <-quotesIn

		log.Trace("quote [" + quote.Instrument + "]")

		send(quote)

		atomic.AddUint64(&counter, 1)
	}
}
