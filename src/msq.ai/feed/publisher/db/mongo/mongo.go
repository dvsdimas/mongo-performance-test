package mongo

import (
	"context"
	"fmt"
	prop "github.com/magiconair/properties"
	log "github.com/sirupsen/logrus"
	"msq.ai/data"
	"msq.ai/db/mongo/connection"
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

const duration = 50 * time.Millisecond
const sleepTime = 10 * time.Millisecond

const batchSize = 10 // TODO make configurable

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

	return func() {

		ctxLog.Info("is going to start")

		feedProvider := prop.MustGet(feedProviderName)
		mongodbUrl := prop.MustGet(mongodbUrlName)

		ctxLog.Info(feedProviderName + " = " + feedProvider + ", " +
			mongodbUrlName + " = " + mongodbUrl)

		ctxLog.Info("Connecting to MongoDB ...")

		client, err := connection.CreateMongoConnection(mongodbUrl)

		if err != nil {
			ctxLog.Fatal(err)
		}

		collection := client.Database(dbName).Collection(feedProvider)

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
			var buf [batchSize]*data.Quote
			var ptr = 0

			var pushIfAfterTimeoutOrSize = func() {

				if hasQuotes {

					if time.Since(start) >= duration || ptr >= batchSize {

						mutex.Lock()

						for i := 0; i < ptr; i++ {
							buffer[pointer] = buf[i]
							pointer++

							if pointer >= bufferSize { // TODO
								ctxLog.Fatal("buffer overflow !!!")
							}
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

				//------------------------------------------------------------------------------------------------------

				var bs = size / batchSize

				for i := 0; i <= bs; i++ {

					var quotes []interface{}

					for j := 0; j < batchSize; j++ {

						index := i*batchSize + j

						if index >= size {
							break
						}

						quotes = append(quotes, *buf[index])
						atomic.AddUint64(&counter, 1)
					}

					if quotes == nil {
						break
					}

					res, err := collection.InsertMany(context.TODO(), quotes)

					if err != nil {
						ctxLog.Fatal(err)
					}

					ctxLog.Trace("Inserted IDs", res.InsertedIDs)

				}

				//------------------------------------------------------------------------------------------------------

				//var quotes []interface{}
				//
				//for i := 0; i < size; i++ {
				//	quotes = append(quotes, *buf[i])
				//	atomic.AddUint64(&counter, 1)
				//}
				//
				//res, err := collection.InsertMany(context.TODO(), quotes)
				//
				//if err != nil {
				//	ctxLog.Fatal(err)
				//}
				//
				//ctxLog.Trace("Inserted IDs", res.InsertedIDs)

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

				log.Info("Sending [" + strconv.FormatUint(delta, 10) + "] quotes per second ")
				log.Info("SENT [" + strconv.FormatUint(counter, 10) + "] quotes")

				time.Sleep(1 * time.Second)
			}
		}()

		ctxLog.Info("has been started !")

		signals <- true
	}
}

func splitByBatch(buf []*data.Quote, size int, batch int) (ret [][]interface{}) {

	var bs = size / batchSize

	for i := 0; i <= bs; i++ {

		var quotes []interface{}

		for j := 0; j < batchSize; j++ {

			index := i*batchSize + j

			if index >= size {
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
