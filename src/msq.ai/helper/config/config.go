package config

import (
	prop "github.com/magiconair/properties"
	log "github.com/sirupsen/logrus"
)

func LoadProperties(path string, defaultProp *prop.Properties) *prop.Properties {

	properties, err := prop.LoadFile(path, prop.UTF8)

	if err != nil {

		log.Warn("Cannot find properties file [" + path + "]. Will use default configuration")

		if defaultProp == nil {
			log.Fatal("Default properties hasn't been set !")
		}

		properties = defaultProp
	}

	log.Info("Will use these properties:")

	for k, v := range properties.Map() {
		log.Info("key[" + k + "] value[" + v + "]")
	}

	return properties
}
