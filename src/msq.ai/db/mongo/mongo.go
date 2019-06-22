package mongo

import (
	"context"
	prop "github.com/magiconair/properties"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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

		// connect to DB

		client, err := CreateMongoConnection("mongodb://127.0.0.1:27017")

		if err != nil {
			ctxLog.Fatal(err)
		}

		collection := client.Database("afdafasfasfASDfrefd").Collection("asdfasdasd")

		ctxLog.Info(collection)

		go func() {

			ctxLog.Info("has been started ! Connecting to MongoDB")

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

func CreateMongoConnection(url string) (client *mongo.Client, err error) {

	client, err = mongo.NewClient(options.Client().ApplyURI(url))

	if err != nil {
		return nil, err
	}

	err = client.Connect(context.TODO())

	if err != nil {
		return nil, err
	}

	err = client.Ping(context.TODO(), nil)

	if err != nil {
		return nil, err
	}

	return client, nil
}

func CloseMongoConnection(client *mongo.Client) (err error) {

	if client == nil {
		return nil
	}

	return client.Disconnect(context.TODO())
}
