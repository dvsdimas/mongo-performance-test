package connection

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoPoint struct {
	Client     *mongo.Client
	Collection *mongo.Collection
}

func CreateMongoConnection(url string, dbName string, collection string) (mongoPoint *MongoPoint, err error) {

	var client *mongo.Client

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

	return &MongoPoint{client, client.Database(dbName).Collection(collection)}, nil
}

func CloseMongoConnection(mongoPoint *MongoPoint) (err error) {

	if mongoPoint == nil || mongoPoint.Client == nil {
		return nil
	}

	return mongoPoint.Client.Disconnect(context.TODO())
}

func DropMongoDb(url string, dbName string) (ok bool, err error) {

	var client *mongo.Client

	client, err = mongo.NewClient(options.Client().ApplyURI(url))

	if err != nil {
		return false, err
	}

	err = client.Connect(context.TODO())

	if err != nil {
		return false, err
	}

	err = client.Ping(context.TODO(), nil)

	if err != nil {
		_ = client.Disconnect(context.TODO())
		return false, err
	}

	err = client.Database(dbName).Drop(context.TODO())

	if err != nil {
		_ = client.Disconnect(context.TODO())
		return false, err
	}

	_ = client.Disconnect(context.TODO())

	return true, nil
}
