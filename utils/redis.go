package utils

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// RedisClusterClient gets redis cluster client
func RedisClusterClient() *redis.ClusterClient {

	host := os.Getenv("REDIS_CLUSTER_HOST")
	auth := os.Getenv("REDIS_CLUSTER_PASSWORD")

	opts := redis.ClusterOptions{
		MinIdleConns: 100,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		PoolSize:     100,
		Addrs:        strings.Split(host, ","),
	}

	if len(auth) > 0 {
		opts.Password = auth
	}

	client := redis.NewClusterClient(&opts)

	// Enable tracing instrumentation.
	if err := redisotel.InstrumentTracing(client); err != nil {
		panic(err)
	}

	// Enable metrics instrumentation.
	if err := redisotel.InstrumentMetrics(client); err != nil {
		panic(err)
	}

	_, err := client.Ping(context.TODO()).Result()
	if err != nil {
		// Log the connection string to help with debugging
		log.Fatalf("Failed to ping redis | Address: %s | Auth: %s | Error: %v", host, auth, err)
	}

	return client
}

// RedisClient gets redis client
func RedisClient() *redis.Client {

	host := os.Getenv("REDIS_HOST")
	port := os.Getenv("REDIS_PORT")
	db := os.Getenv("REDIS_DATABASE_NUMBER")
	auth := os.Getenv("REDIS_PASSWORD")

	// --- Debugging Check: Ensure host and port are set
	if host == "" || port == "" {
		log.Fatal("FATAL: Redis host or port environment variables are not set.")
	}

	dbNumber, err := strconv.Atoi(db)
	if err != nil {

		log.Printf("Could not convert DB number, defaulting to 0: %v", err)
		dbNumber = 0
	}

	// The `Addr` field expects a "host:port" string.
	addr := fmt.Sprintf("%s:%s", host, port)

	opts := redis.Options{
		MinIdleConns: 100,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		PoolSize:     100,
		Addr:         addr,
		DB:           dbNumber,
	}

	if len(auth) > 0 {
		opts.Password = auth
	}

	client := redis.NewClient(&opts)

	// Enable tracing instrumentation.
	if err := redisotel.InstrumentTracing(client); err != nil {
		panic(err)
	}

	// Enable metrics instrumentation.
	if err := redisotel.InstrumentMetrics(client); err != nil {
		panic(err)
	}

	_, err = client.Ping(context.TODO()).Result()
	if err != nil {
		// Log the connection string to help with debugging
		log.Fatalf("Failed to ping redis | Address: %s | Auth: %s | Error: %v", addr, auth, err)
	}

	return client
}

// GetRedisKey get saved key from redis
func GetRedisKey(ctx context.Context, conn *redis.ClusterClient, key string) (string, error) {

	var data string
	data, err := conn.Get(ctx, key).Result()
	if err != nil {

		//return data, fmt.Errorf("error getting key %s: %v", key, err)
		return "", err
	}

	return data, err

}

// SetRedisKeyWithExpiry saves key to redis with TTL value
func SetRedisKeyWithExpiry(ctx context.Context, conn *redis.ClusterClient, key string, value string, seconds int) error {

	_, err := conn.Set(ctx, key, value, time.Second*time.Duration(seconds)).Result()
	if err != nil {

		v := value

		if len(v) > 15 {

			v = v[0:12] + "..."
		}

		log.Printf("error saving redisKey %s | %s", key, err.Error())
		return fmt.Errorf("error setting key %s to %s: %v", key, v, err)
	}

	return err
}

// SetRedisKey saves key to redis without expiry
func SetRedisKey(ctx context.Context, conn *redis.ClusterClient, key string, value string) error {

	_, err := conn.Set(ctx, key, value, 0).Result()
	if err != nil {

		v := string(value)

		if len(v) > 15 {

			v = v[0:12] + "..."
		}

		log.Printf("error saving redisKey %s | %s", key, err.Error())
		return fmt.Errorf("error setting key %s to %s: %v", key, v, err)
	}

	return err
}

// DeleteRedisKey deletes a saved redis keys
func DeleteRedisKey(ctx context.Context, conn *redis.ClusterClient, key string) error {

	_, err := conn.Del(ctx, key).Result()
	if err != nil {

		log.Printf("error deleting redisKey %s error %s", key, err.Error())
		return fmt.Errorf("error deleting key %s | %s", key, err)
	}

	return err
}

// DeleteKeysByPattern deletes a set of keys matching the supplied pattern
func DeleteKeysByPattern(ctx context.Context, conn *redis.ClusterClient, keyPattern string) error {

	iter := conn.Scan(ctx, 0, keyPattern, 0).Iterator()
	for iter.Next(ctx) {

		DeleteRedisKey(ctx, conn, iter.Val())
	}

	if err := iter.Err(); err != nil {

		log.Printf("error iteration error deleteing keys %s | %s", keyPattern, err.Error())
		return err
	}

	return nil
}

func RedisKeyExists(ctx context.Context, conn *redis.ClusterClient, key string) (bool, error) {

	check, err := conn.Exists(ctx, key).Result()
	if err != nil {

		log.Printf("error checking if redisKey %s exists | %s", key, err.Error())
		return false, err
	}

	return check > 0, nil
}

func GetAllKeysByPattern(ctx context.Context, conn *redis.ClusterClient, keyPattern string) []string {

	var keys []string
	iter := conn.Scan(ctx, 0, keyPattern, 0).Iterator()
	for iter.Next(ctx) {

		keys = append(keys, iter.Val())
	}

	return keys
}
