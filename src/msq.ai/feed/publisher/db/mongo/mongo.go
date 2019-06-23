package mongo

import (
	"context"
	"fmt"
	prop "github.com/magiconair/properties"
	log "github.com/sirupsen/logrus"
	"msq.ai/data"
	mongo "msq.ai/db/mongo/connection"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const name string = "MongoFeedPublisher"
const id string = "ID"

const feedProviderName string = "feed.provider"
const mongodbUrlName string = "mongodb.url"

const dbName string = "msq"

const bufferSize = 10000
const smallBufferSize = 100

const duration = 50 * time.Millisecond
const sleepTime = 10 * time.Millisecond

const batchSizeName = "mongodb.batchSize"

func MakeMongoConnector(prop *prop.Properties, in <-chan *data.Quote, signals chan<- bool) func() {

	ctxLog := log.WithFields(log.Fields{id: name})

	if prop == nil {
		ctxLog.Fatal("properties is nil !")
	}

	if in == nil {
		ctxLog.Fatal("in chanel is nil !")
	}

	if signals == nil {
		ctxLog.Fatal("in chanel is nil !")
	}

	parseInt := func(str string) int {

		var n int
		var err error

		if n, err = strconv.Atoi(str); err != nil {
			ctxLog.Fatal("Cannot parse int [" + str + "]")
		}

		return n
	}

	return func() {

		ctxLog.Info("is going to start")

		feedProvider := prop.MustGet(feedProviderName)
		mongodbUrl := prop.MustGet(mongodbUrlName)
		batchSize := parseInt(prop.MustGet(batchSizeName))

		ctxLog.Info(feedProviderName + " = " + feedProvider + ", " +
			mongodbUrlName + " = " + mongodbUrl + ", " +
			batchSizeName + " = " + strconv.Itoa(batchSize))

		//--------------------------------------------------------------------------------------------------------------

		var mongoPoint *mongo.MongoPoint

		reconnect := func() {

			err := mongo.CloseMongoConnection(mongoPoint)

			if err != nil {
				ctxLog.Error(err)
			}

			for {

				mongoPoint, err = mongo.CreateMongoConnection(mongodbUrl, dbName, feedProvider)

				if err != nil {
					ctxLog.Error(err)
					time.Sleep(1 * time.Second)
					continue
				}

				break
			}
		}

		writeToMongo := func(bufQuotes []interface{}) {

			for {
				_, err := mongoPoint.Collection.InsertMany(context.TODO(), bufQuotes)

				if err == nil {
					break
				}

				ctxLog.Error(err)
				reconnect()
				continue
			}
		}

		//--------------------------------------------------------------------------------------------------------------

		ctxLog.Info("Connecting to MongoDB ...")

		reconnect()

		ctxLog.Info("Connected to MongoDB successfully")

		//--------------------------------------------------------------------------------------------------------------

		var buffer [bufferSize]*data.Quote
		var pointer = 0
		var mutex = &sync.Mutex{}
		var counter uint64 = 0

		//--------------------------------------------------------------------------------------------------------------

		receiver := func() {

			var hasQuotes = false
			var start time.Time
			var buf [smallBufferSize]*data.Quote
			var ptr = 0

			var pushIfAfterTimeoutOrSize = func() {

				if hasQuotes {

					if time.Since(start) >= duration || ptr >= smallBufferSize {

						mutex.Lock()

						for i := 0; i < ptr; i++ {

							if pointer >= bufferSize { // TODO
								ctxLog.Fatal("buffer overflow !!!")
							}

							buffer[pointer] = buf[i]
							pointer++
						}

						mutex.Unlock()

						ptr = 0
						hasQuotes = false
						return
					}
				}

				time.Sleep(sleepTime)
			}

			for {
				select {

				case quote := <-in:
					{

						ctxLog.Trace("quote [" + fmt.Sprintf("%#v", *quote) + "]")

						if !hasQuotes {
							hasQuotes = true
							start = time.Now()
						}

						buf[ptr] = quote
						ptr++

						pushIfAfterTimeoutOrSize()
					}
				default:
					{
						pushIfAfterTimeoutOrSize()
					}

				}
			}

		}

		//--------------------------------------------------------------------------------------------------------------

		publisher := func() {

			var buf [bufferSize]*data.Quote
			var size = 0

			for {

				mutex.Lock()

				if pointer > 0 {

					for i := 0; i < pointer; i++ {
						buf[i] = buffer[i]
						size++
					}

					pointer = 0
				}

				mutex.Unlock()

				if size == 0 {
					time.Sleep(sleepTime)
					continue
				}

				for _, b := range splitByBatch(buf[0:size], batchSize) {

					writeToMongo(b)

					atomic.AddUint64(&counter, uint64(len(b)))
				}

				size = 0
			}

		}

		//--------------------------------------------------------------------------------------------------------------

		go receiver()
		go publisher()

		go func() {

			var prev uint64 = 0

			for {

				var delta uint64

				delta, prev = counter-prev, counter

				ctxLog.Info("Sending [" + strconv.FormatUint(delta, 10) + "] quotes per second ")
				ctxLog.Trace("SENT [" + strconv.FormatUint(counter, 10) + "] quotes")

				time.Sleep(1 * time.Second)
			}
		}()

		ctxLog.Info("has been started !")

		signals <- true
	}
}

func splitByBatch(buf []*data.Quote, batch int) (ret [][]interface{}) {

	var bs = len(buf) / batch

	for i := 0; i <= bs; i++ {

		var quotes []interface{}

		for j := 0; j < batch; j++ {

			index := i*batch + j

			if index >= len(buf) {
				break
			}

			quotes = append(quotes, *buf[index])
		}

		if quotes == nil {
			break
		}

		ret = append(ret, quotes)
	}

	return ret
}
