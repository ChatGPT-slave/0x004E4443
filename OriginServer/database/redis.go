package database

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
)

func InitRedis(url string) (*redis.Client, error) {
	pOpts, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}

	return redis.NewClient(pOpts), nil
}

func putObject2Redis(ctx context.Context, keyName string, object []byte, pRedisClient *redis.Client) {
	ttl := 24 * time.Hour
	err := pRedisClient.Set(ctx, keyName, object, ttl).Err()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
}

func PutMeta2Redis(ctx context.Context, keyName string, metadata MetadataObject, pRedisClient *redis.Client) {
	metadataBin, err := msgpack.Marshal(metadata)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	putObject2Redis(ctx, keyName, metadataBin, pRedisClient)
}

func PutMeta2RedisAsync(ctx context.Context, keyName string, metadata MetadataObject, pRedisClient *redis.Client) {
	go PutMeta2Redis(ctx, keyName, metadata, pRedisClient)
}

func PutData2Redis(ctx context.Context, keyName string, objectdata []byte, pRedisClient *redis.Client) {
	putObject2Redis(ctx, keyName, objectdata, pRedisClient)
}

func PutData2RedisAsync(ctx context.Context, keyName string, objectdata []byte, pRedisClient *redis.Client) {
	go PutData2Redis(ctx, keyName, objectdata, pRedisClient)
}

func getObjectFromRedis(key string, pRedisClient *redis.Client) ([]byte, error) {
	var result []byte
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := pRedisClient.Get(ctx, key).Bytes()
	return result, err
}

func GetMetaObjectFromRedis(metaKey string, pRedisClient *redis.Client) (MetadataObject, error) {
	var metadata MetadataObject
	bin, err := getObjectFromRedis(metaKey, pRedisClient)
	if err != nil {
		return metadata, err
	}
	err = msgpack.Unmarshal(bin, &metadata)
	return metadata, nil
}

func GetDataObjectFromRedis(dataKey string, pRedisClient *redis.Client) ([]byte, error) {
	return getObjectFromRedis(dataKey, pRedisClient)
}

func GetBitsPipeline(ctx context.Context, key string, positions []int64, pRedisClient *redis.Client) ([]int64, error) {
	pipe := pRedisClient.Pipeline()
	cmds := make([]*redis.IntCmd, len(positions))

	for i, pos := range positions {
		cmds[i] = pipe.GetBit(ctx, key, pos)
	}

	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, err
	}

	results := make([]int64, len(positions))
	for i, cmd := range cmds {
		val, err := cmd.Result()
		if err != nil {
			return nil, err
		}
		results[i] = val
	}

	return results, nil
}

func SetBitsPipeline(ctx context.Context, key string, positions []int64, value int, pRedisClient *redis.Client) error {
	pipe := pRedisClient.Pipeline()
	for _, pos := range positions {
		pipe.SetBit(ctx, key, pos, value)
	}

	_, err := pipe.Exec(ctx)
	return err
}

/*
docker exec -it cdn_origin_redis redis-cli -a MyStrongPassword
*/
