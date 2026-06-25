package database

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

type MetadataObject struct {
	ID          bson.ObjectID `bson:"_id,omitempty"`
	BucketName  string        `bson:"bucket_name"`
	ObjectName  string        `bson:"object_name"`
	ObjectType  string        `bson:"object_type"`
	ObjectSize  int64         `bson:"object_size"`
	ETag        string        `bson:"etag"`
	CreatedAt   time.Time     `bson:"created_at"`
	IsFastCache bool          `bson:"is_fast_cache"`
}
