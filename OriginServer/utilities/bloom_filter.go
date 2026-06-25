package utilities

import (
	"OriginServer/database"
	"context"
	"slices"

	"github.com/redis/go-redis/v9"
	"github.com/zeebo/xxh3"
)

const (
	BloomFilterSize = 10 * 1024 * 1024
	HashCount       = 7
)

func getBitPositions(key string) []int64 {
	data := []byte(key)

	hash128 := xxh3.Hash128(data)

	h1 := hash128.Hi
	h2 := hash128.Lo

	positions := make([]int64, HashCount)

	for i := range HashCount {
		pos := (h1 + uint64(i)*h2) % uint64(BloomFilterSize)
		positions[i] = int64(pos)
	}

	return positions
}

func AddToBloomFilter(ctx context.Context, bloomKey, objectName string, pRedisClient *redis.Client) error {
	positions := getBitPositions(objectName)
	return database.SetBitsPipeline(ctx, bloomKey, positions, 1, pRedisClient)
}

func IsInBloomFilter(ctx context.Context, bloomKey, objectName string, pRedisClient *redis.Client) bool {
	positions := getBitPositions(objectName)

	bits, err := database.GetBitsPipeline(ctx, bloomKey, positions, pRedisClient)
	if err != nil {
		return false
	}

	return !slices.Contains(bits, 0)
}
