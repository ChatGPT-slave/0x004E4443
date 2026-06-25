package database

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func InitMongodb(uri string) (*mongo.Client, error) {
	pOpts := options.Client().ApplyURI(uri)
	return mongo.Connect(pOpts)
}

func PutObject2Mongodb(ctx context.Context, dbName, collectionName string, metadataObject MetadataObject, pMongoClient *mongo.Client) (*mongo.UpdateResult, error) {
	collection := pMongoClient.Database(dbName).Collection(collectionName)

	filter := bson.M{
		"bucket_name": metadataObject.BucketName,
		"object_name": metadataObject.ObjectName,
	}
	opts := options.Replace().SetUpsert(true)

	return collection.ReplaceOne(ctx, filter, metadataObject, opts)
}

func PutObject2MongodbAsync(ctx context.Context, dbName, collectionName string, metadataObject MetadataObject, pMongoClient *mongo.Client) {
	go PutObject2Mongodb(ctx, dbName, collectionName, metadataObject, pMongoClient)
}

func GetObjectFromMongo(ctx context.Context, dbName, collectionName string, bucketName, objectName string, pMongoClient *mongo.Client) (MetadataObject, error) {
	collection := pMongoClient.Database(dbName).Collection(collectionName)
	filter := bson.M{
		"bucket_name": bucketName,
		"object_name": objectName,
	}
	var metadata MetadataObject
	err := collection.FindOne(ctx, filter).Decode(&metadata)
	return metadata, err
}
