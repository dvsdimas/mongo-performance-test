package generator

import (
	log "github.com/sirupsen/logrus"
	"math/rand"
	"msq.ai/constants"
	"msq.ai/data"
	"strconv"
	"time"
)

const id string = "ID"
const name string = "FeedGenerator"
const instrument string = "INSTR"

func MakeFeedGenerator(instrumentsCount int, quotesPerSecond int, out chan<- *data.Quote, in <-chan bool) func() {

	ctxLog := log.WithFields(log.Fields{id: name})

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

		ctxLog.Info(constants.InstrumentsCountName + " = " + strconv.Itoa(instrumentsCount) +
			", " + constants.QuotesPerSecondName + " = " + strconv.Itoa(quotesPerSecond))

		instruments := make([]string, instrumentsCount)

		for i := 0; i < instrumentsCount; i++ {
			instruments[i] = instrument + strconv.Itoa(i)
		}

		oneSecondInstruments := make([]*data.Quote, 0)

		for i := 0; i < quotesPerSecond; i++ {
			for j := 0; j < instrumentsCount; j++ {
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
