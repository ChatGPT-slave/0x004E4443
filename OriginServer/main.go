package main

import (
	"OriginServer/database"
	"OriginServer/storage"
	"OriginServer/utilities"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type UploadSessionRequest struct {
	FileName    string `json:"file_name"`
	Size        int64  `json:"size"`
	ContentType string `json:"content_type"`
}

var (
	g_maxSizeOfSmallFile = 2 * 1024 * 1024
	g_minioBucketName    = "cdn-origin"
	pMinioClient         *minio.Client

	g_redisMetaKeyFormat  = "cdn:meta:%s"
	g_redisDataKeyFormat  = "cdn:data:%s"
	g_redisBloomFilterKey = "cdn:bloom:filter"
	pRedisClient          *redis.Client

	g_mongoDbName    = "cdn_origin"
	g_collectionName = "metadata"
	pMongoClient     *mongo.Client
)

func OnDownloadFileController(response http.ResponseWriter, pRequest *http.Request, objectName string) {
	bucketName := g_minioBucketName

	// Get meta object
	isExistsInRedis := true
	isNeedUpdateInRedis := false
	metadata, err := database.GetMetaObjectFromRedis(fmt.Sprintf(g_redisMetaKeyFormat, objectName), pRedisClient)
	if err != nil {
		// Lookup in mongodb
		metadata, err = database.GetObjectFromMongo(context.Background(), g_mongoDbName, g_collectionName, bucketName, objectName, pMongoClient)
		if err != nil {
			http.Error(response, "File not found", http.StatusNotFound)
			return
		}
		isExistsInRedis = false
	}

	// Get data object
	var reader io.Reader
	var pMinioObject io.ReadCloser

	if metadata.IsFastCache == true {
		var dataObject []byte
		dataObject, err = database.GetDataObjectFromRedis(fmt.Sprintf(g_redisDataKeyFormat, objectName), pRedisClient)
		if err == nil {
			reader = bytes.NewReader(dataObject)
		} else {
			metadata.IsFastCache = false
		}
	}

	if metadata.IsFastCache == false {
		pMinioObject, err = storage.GetObjectFromMinio(context.Background(), bucketName, objectName, pMinioClient)
		if err != nil {
			http.Error(response, "File not found", http.StatusNotFound)
			return
		}
		defer pMinioObject.Close()
		reader = pMinioObject
	}

	response.Header().Set("Content-Type", metadata.ObjectType)
	response.Header().Set("Content-Length", strconv.FormatInt(metadata.ObjectSize, 10))
	response.Header().Set("ETag", metadata.ETag)

	// re-cache data object
	if metadata.IsFastCache == false && metadata.ObjectSize <= int64(g_maxSizeOfSmallFile) {
		var cdnBuffer bytes.Buffer
		teeReader := io.TeeReader(reader, &cdnBuffer)

		_, err = io.Copy(response, teeReader)
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		database.PutData2Redis(context.Background(), fmt.Sprintf(g_redisDataKeyFormat, objectName), cdnBuffer.Bytes(), pRedisClient)
		metadata.IsFastCache = true
		isNeedUpdateInRedis = true
	} else {
		_, err = io.Copy(response, reader)
		if err != nil {
			fmt.Println(err.Error())
		}
	}

	if !isExistsInRedis || isNeedUpdateInRedis {
		database.PutMeta2RedisAsync(context.Background(), fmt.Sprintf(g_redisMetaKeyFormat, objectName), metadata, pRedisClient)
	}
}

func OnObjectHeaderController(response http.ResponseWriter, pRequest *http.Request) {

}

func OnCreateUploadSession(response http.ResponseWriter, pRequest *http.Request) {
	var req UploadSessionRequest
	err := json.NewDecoder(pRequest.Body).Decode(&req)
	if err != nil {
		http.Error(response, "Format json is error", http.StatusBadRequest)
		return
	}

	if req.FileName == "" || req.Size <= 0 {
		http.Error(response, "Info json is wrong", http.StatusBadRequest)
		return
	}
	const smallSizeLimited = 20 * 1024 * 1024

}

func OnUploadFileController(response http.ResponseWriter, pRequest *http.Request) {
	// limited upload size
	pRequest.Body = http.MaxBytesReader(response, pRequest.Body, 20<<20)
	err := pRequest.ParseMultipartForm(20 << 20)
	if err != nil {
		var httpcode int
		if err == http.ErrNotMultipart {
			httpcode = http.StatusBadRequest
		} else {
			httpcode = http.StatusRequestEntityTooLarge
		}
		http.Error(response, err.Error(), httpcode)
		return
	}

	// Get file stream data
	file, pHeader, err := pRequest.FormFile("file")
	if err != nil {
		http.Error(response, err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	bucketName := g_minioBucketName
	objectName := pHeader.Filename
	objectType := ""
	objectSize := pHeader.Size

	// Read 512 bytes first to check content type. If file size <= 2Mb, read all of file to cache in redis
	bytesWantRead := 512
	if objectSize <= int64(g_maxSizeOfSmallFile) {
		bytesWantRead = int(objectSize)
	}

	objectData := make([]byte, bytesWantRead)
	_, err = file.Read(objectData)
	if err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}

	objectType = http.DetectContentType(objectData)

	// put object to minio
	metadata, err := storage.PutObject2Minio(pRequest.Context(), bucketName, objectName, objectType, objectSize, file, pMinioClient)
	if err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}

	// put metadata to mongo db
	_, err = database.PutObject2Mongodb(pRequest.Context(), g_mongoDbName, g_collectionName, metadata, pMongoClient)
	if err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		go storage.RemoveObjectFromMinio(context.Background(), bucketName, objectName, pMinioClient)
		return
	}

	// Put data, metadata async
	database.PutMeta2RedisAsync(context.Background(), fmt.Sprintf(g_redisMetaKeyFormat, objectName), metadata, pRedisClient)
	if bytesWantRead == int(objectSize) {
		database.PutData2RedisAsync(context.Background(), fmt.Sprintf(g_redisDataKeyFormat, objectName), objectData, pRedisClient)
		metadata.IsFastCache = true
	}
	response.WriteHeader(http.StatusCreated)
}

func main() {
	var err error
	// Init Minio
	pMinioClient, err = storage.InitMinIO("127.0.0.1:9000", "minioadmin", "minioadmin", false)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	// Init Redis
	pRedisClient, err = database.InitRedis("redis://:MyStrongPassword@127.0.0.1:6379/0?protocol=3")
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	// Init Mongo
	pMongoClient, err = database.InitMongodb("mongodb://mongodbadmin:mongodbadmin@localhost:27017")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	_ = pMongoClient.Ping(context.Background(), nil)

	// middleware to check bloom filter
	CheckBloomFilterMiddleware := func(controller func(w http.ResponseWriter, r *http.Request, objectName string)) func(w http.ResponseWriter, r *http.Request) {
		return func(w http.ResponseWriter, r *http.Request) {
			// Get query value (?file=abc)
			objectName := r.URL.Query().Get("file")

			// Create 0.5s timeout
			ctx := r.Context()
			timeout, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()

			// Check bloom filter
			isExists := utilities.IsInBloomFilter(timeout, g_redisBloomFilterKey, objectName, pRedisClient)
			if isExists == false {
				http.Error(w, "File not found", http.StatusNotFound)
				return
			}

			// Begin download object
			controller(w, r, objectName)
		}
	}

	http.HandleFunc("GET /download-file", CheckBloomFilterMiddleware(OnDownloadFileController))
	http.HandleFunc("POST /upload-file", OnUploadFileController)

	//http.HandleFunc("POST /api/create-upload-session", OnCreateUploadSession)
	//http.HandleFunc("HEAD /", HeadController)

	fmt.Println("Server is listening on port 11311")
	err = http.ListenAndServe("127.0.0.1:11311", nil)
	if err != nil {
		panic(err)
	}
}
