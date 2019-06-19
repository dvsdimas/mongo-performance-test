package generator

import (
	prop "github.com/magiconair/properties"
	log "github.com/sirupsen/logrus"
)

const instrumentsCount string = "instruments.count"
const name string = "FeedGenerator"
const id string = "ID"

func MakeFeedGenerator(prop *prop.Properties) func() {

	ctxLog := log.WithFields(log.Fields{id: name})

	return func() {

		ctxLog.Info("is going to start")

		iCount := prop.MustGet(instrumentsCount)

		ctxLog.Info("[" + instrumentsCount + "] is [" + iCount + "]")

		go func() {
			ctxLog.Info("has been started !")
		}()

	}
}
