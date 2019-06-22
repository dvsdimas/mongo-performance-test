package mongo

import (
	"fmt"
	prop "github.com/magiconair/properties"
	log "github.com/sirupsen/logrus"
	"msq.ai/data"
	"msq.ai/db/mongo/connection"
	"time"
)

const name string = "MongoFeedPublisher"
const id string = "ID"

const feedProviderName string = "feed.provider"
const mongodbUrlName string = "mongodb.url"

const dbName string = "msq"

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

		ctxLog.Trace(collection) // TODO

		ctxLog.Info("Connected to MongoDB successfully")

		receiver := func() {

			signals <- true

			for {
				quote := <-in

				ctxLog.Trace("quote [" + fmt.Sprintf("%#v", *quote) + "]")
			}

		}

		publisher := func() {

			for {

				ctxLog.Info("Pinging MongoDB")

				err := connection.PingMongoConnection(client)

				if err != nil {
					ctxLog.Fatal(err)
				}

				time.Sleep(1000 * time.Millisecond)
			}

		}

		go receiver()
		go publisher()

		ctxLog.Info("has been started !")
	}
}
