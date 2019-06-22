package connection

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

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

func PingMongoConnection(client *mongo.Client) (err error) {

	return client.Ping(context.TODO(), nil)
}
