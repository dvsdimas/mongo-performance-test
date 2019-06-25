package generator

import (
	prop "github.com/magiconair/properties"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"msq.ai/data"
	"strconv"
	"time"
)

const id string = "ID"
const name string = "FeedGenerator"

const instrumentsCount string = "instruments.count"
const quotesPerSecond string = "quotes.per.second"

const instrument string = "INSTR"

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

		iCount := parseInt(prop.MustGet(instrumentsCount))
		perSec := parseInt(prop.MustGet(quotesPerSecond))

		ctxLog.Info(instrumentsCount + " = " + strconv.Itoa(iCount) +
			", " + quotesPerSecond + " = " + strconv.Itoa(perSec))

		instruments := make([]string, iCount)

		for i := 0; i < iCount; i++ {
			instruments[i] = instrument + strconv.Itoa(i)
		}

		oneSecondInstruments := make([]*data.Quote, 0)

		for i := 0; i < perSec; i++ {
			for j := 0; j < iCount; j++ {
				oneSecondInstruments = append(oneSecondInstruments, &data.Quote{Instrument: instruments[j]})
			}
		}

		batch := len(oneSecondInstruments) / 10

		random := rand.New(rand.NewSource(time.Now().Unix()))

		var id uint64 = 1

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

				for i := 0; i < 10; i++ {

					start := time.Now()

					//------------------------

					for j := 0; j < batch; j++ {

						index := i*batch + j

						q := data.Quote(*oneSecondInstruments[index])

						q.Time = time.Now().UnixNano()
						q.Ask = 1 + random.Float64()
						q.Bid = 1 + random.Float64()
						q.Id = id

						id++

						send(&q)
					}

					//------------------------

					duration := time.Since(start)

					needSleep := 100*time.Millisecond - duration

					time.Sleep(needSleep)
				}
			}

		}()

	}
}
