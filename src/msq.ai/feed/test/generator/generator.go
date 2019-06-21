package generator

import (
	prop "github.com/magiconair/properties"
	log "github.com/sirupsen/logrus"
	"msq.ai/data"
	"time"
)

const instrumentsCount string = "instruments.count"
const name string = "FeedGenerator"
const feedSourceId string = "feed.source.id"
const id string = "ID"

func MakeFeedGenerator(prop *prop.Properties, out chan<- *data.Quote, in <-chan bool) func() {

	ctxLog := log.WithFields(log.Fields{id: name})

	if prop == nil {
		ctxLog.Fatal("properties is nil !")
	}

	if out == nil {
		ctxLog.Fatal("out chanel is nil !")
	}

	if in == nil {
		ctxLog.Fatal("signals chanel is nil !")
	}

	send := func(quote *data.Quote) {
		select {
		case out <- quote:
		default:
			ctxLog.Fatal("out chanel buffer is overflowed !!!")
		}
	}

	return func() {

		ctxLog.Info("is going to start")

		iCount := prop.MustGet(instrumentsCount)
		sourceId := prop.MustGet(feedSourceId)

		ctxLog.Info(instrumentsCount+" = "+iCount+", "+feedSourceId+" = ", sourceId)

		go func() {

			ctxLog.Info("has been started ! Waiting signal to start")

			for {
				s := <-in

				if s == true {
					break
				}
			}

			ctxLog.Info("Going to start feeding")

			for {

				send(&data.Quote{
					Source:     0,
					Instrument: "Asddd",
					Bid:        0,
					Ask:        0,
					Time:       time.Now().UnixNano(),
				})

				time.Sleep(200 * time.Millisecond)
			}

		}()

	}
}
