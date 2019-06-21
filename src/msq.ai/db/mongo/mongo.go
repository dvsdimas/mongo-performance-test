package mongo

import (
	prop "github.com/magiconair/properties"
	log "github.com/sirupsen/logrus"
	"msq.ai/data"
)

const name string = "MongoConnector"
const id string = "ID"

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

		//iCount := prop.MustGet(instrumentsCount)
		//sourceId := prop.MustGet(feedSourceId)

		//ctxLog.Info(instrumentsCount+" = "+iCount+", "+feedSourceId+" = ", sourceId)

		go func() {

			ctxLog.Info("has been started ! Connecting to MongoDB")

			// TODO connect to DB

			signals <- true

			for {

				quote := <-in

				ctxLog.Trace("quote [" + quote.Instrument + "]")

				//send(&data.Quote{
				//	Source:     0,
				//	Instrument: "Asddd",
				//	Bid:        0,
				//	Ask:        0,
				//	Time:       0,
				//})

				//time.Sleep(1 * time.Second)
			}

		}()

	}
}
