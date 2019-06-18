package main

import (
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

func init() {

	log.SetFormatter(&log.TextFormatter{
		DisableColors: true,
		FullTimestamp: true,
	})

	log.SetOutput(os.Stdout)

	log.SetLevel(log.TraceLevel)
}

func main() {

	for {
		log.Debug("Hello world !!!")

		contextLogger := log.WithFields(log.Fields{
			"common": "this is a common field",
			"other":  "I also should be logged always",
		})

		contextLogger.Info("I'll be logged with common and other field")

		time.Sleep(1 * time.Second)
	}

}
