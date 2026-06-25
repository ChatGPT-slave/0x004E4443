package storage

import (
	"OriginServer/database"
	"context"
	"io"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func InitMinIO(endpoint, accessKey, secretKey string, useSSL bool) (*minio.Client, error) {
	pOpts := &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	}

	return minio.New(endpoint, pOpts)
}

func CreateBucket(ctx context.Context, bucketName string, pMinioClient *minio.Client) error {
	isExists, err := pMinioClient.BucketExists(ctx, bucketName)
	if err != nil {
		return err
	}

	if isExists == false {
		err = pMinioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		if err != nil {
			return err
		}
	}
	return nil
}

func PutObject2Minio(ctx context.Context, bucketName, objectName, objectType string, objectSize int64, reader io.Reader, pMinioClient *minio.Client) (database.MetadataObject, error) {
	uploadInfo, err := pMinioClient.PutObject(ctx, bucketName, objectName, reader, objectSize, minio.PutObjectOptions{
		ContentType: objectType,
	})

	if err != nil {
		return database.MetadataObject{}, err
	}

	metadata := database.MetadataObject{
		BucketName:  uploadInfo.Bucket,
		ObjectName:  objectName,
		ObjectType:  objectType,
		ObjectSize:  uploadInfo.Size,
		ETag:        uploadInfo.ETag,
		CreatedAt:   time.Now(),
		IsFastCache: false,
	}

	return metadata, nil
}

func GetObjectFromMinio(ctx context.Context, bucketName string, objectName string, pMinioClient *minio.Client) (*minio.Object, error) {
	return pMinioClient.GetObject(ctx, bucketName, objectName, minio.GetObjectOptions{})
}

func RemoveObjectFromMinio(ctx context.Context, bucketName string, objectName string, pMinioClient *minio.Client) error {
	return pMinioClient.RemoveObject(ctx, bucketName, objectName, minio.RemoveObjectOptions{})
}
