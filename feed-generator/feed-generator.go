package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"msq.ai/constants"
	"msq.ai/data"
	feeder "msq.ai/feed/publisher/db/mongo"
	"msq.ai/feed/test/generator"
	"msq.ai/helper/config"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

const propFileName string = "feed-generator.properties"
const bufferSize int32 = 100000

func init() {

	log.SetFormatter(&log.TextFormatter{
		DisableColors: true,
		FullTimestamp: true,
	})

	log.SetOutput(os.Stdout)

	log.SetLevel(log.InfoLevel)
}

func main() {

	parseInt := func(str string) int {

		var n int
		var err error

		if n, err = strconv.Atoi(str); err != nil {
			log.Fatal("Cannot parse int [" + str + "]")
		}

		return n
	}

	log.Info("feed-publisher is starting")

	pwd, _ := os.Getwd()

	log.Debug("Current folder is [" + pwd + "]")

	properties := config.LoadProperties(propFileName, nil)

	feedProvider := properties.MustGet(constants.FeedProviderName)
	mongodbUrl := properties.MustGet(constants.MongodbUrlName)
	batchSize := parseInt(properties.MustGet(constants.BatchSizeName))
	dbName := properties.MustGet(constants.DbName)

	quotesPerSecond := parseInt(properties.MustGet(constants.QuotesPerSecondName))
	instrumentsCount := parseInt(properties.MustGet(constants.InstrumentsCountName))

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

	generator.MakeFeedGenerator(instrumentsCount, quotesPerSecond, quotesIn, signals)()

	feeder.MakeMongoConnector(mongodbUrl, dbName, feedProvider, batchSize, quotesOut, signals)()

	var counter uint64 = 0

	go func() {

		var prev uint64 = 0

		for {

			var delta uint64

			delta, prev = counter-prev, counter

			log.Info("Producing [" + strconv.FormatUint(delta, 10) + "] quotes per second ")
			log.Trace("GEN [" + strconv.FormatUint(counter, 10) + "] quotes")

			time.Sleep(1 * time.Second)
		}
	}()

	for {

		quote := <-quotesIn

		log.Trace("quote [" + fmt.Sprintf("%#v", quote) + "]")

		send(quote)

		atomic.AddUint64(&counter, 1)
	}
}
