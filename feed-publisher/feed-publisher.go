package main

import (
	//"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	//"time"
	prop "github.com/magiconair/properties"
)

const propFileName string = "feed-publisher.properties"

var defaultProperties = map[string]string{"key1": "value1",
	"key2": "value2"}

func init() {

	log.SetFormatter(&log.TextFormatter{
		DisableColors: true,
		FullTimestamp: true,
	})

	log.SetOutput(os.Stdout)

	log.SetLevel(log.TraceLevel)
}

func loadProperties(path string, defaultProp *prop.Properties) *prop.Properties {

	properties, err := prop.LoadFile(path, prop.UTF8)

	if err != nil {

		log.Warn("Cannot find properties file [" + path + "]. Will use default configuration")

		properties = defaultProp
	}

	log.Info("Will use these properties:")

	for k, v := range properties.Map() {
		log.Info("key[" + k + "] value[" + v + "]")
	}

	return properties
}

// msq.ai

func main() {

	log.Info("feed-publisher is starting") // msq.ai

	properties := loadProperties(propFileName, prop.LoadMap(defaultProperties))

	if properties == nil {
		log.Fatal("Properties has been set !!!")
	}

}

//contextLogger := log.WithFields(log.Fields{
//	"common": "this is a common field",
//	"other":  "I also should be logged always",
//})
//
//contextLogger.Info("I'll be logged with common and other field")
//
//time.Sleep(1 * time.Second)
