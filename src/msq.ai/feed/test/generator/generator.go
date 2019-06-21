package generator

import (
	prop "github.com/magiconair/properties"
	log "github.com/sirupsen/logrus"
	"msq.ai/data"
	"strconv"
	"time"
)

const id string = "ID"
const name string = "FeedGenerator"

const instrumentsCount string = "instruments.count"
const feedSourceId string = "feed.source.id"
const quotesPerSecond string = "quotes.per.second"

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

	parseInt := func(str string) int64 {

		var n int64
		var err error

		if n, err = strconv.ParseInt(str, 10, 64); err != nil {
			ctxLog.Fatal("signals chanel is nil !")
		}

		return n
	}

	return func() {

		ctxLog.Info("is going to start")

		iCount := parseInt(prop.MustGet(instrumentsCount))
		sourceId := parseInt(prop.MustGet(feedSourceId))
		perSec := parseInt(prop.MustGet(quotesPerSecond))

		ctxLog.Info(instrumentsCount+" = "+strconv.FormatInt(iCount, 10)+
			", "+feedSourceId+" = ", strconv.FormatInt(sourceId, 10)+
			", "+quotesPerSecond+" = ", strconv.FormatInt(perSec, 10))

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
					Instrument: "EUR/USD",
					Bid:        1.12345,
					Ask:        1.23456,
					Time:       time.Now().UnixNano(),
				})

				time.Sleep(1000 * time.Millisecond)
			}

		}()

	}
}
