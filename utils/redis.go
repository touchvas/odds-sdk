package utils

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"log"
	"os"
	"strings"
	"time"
)

// RedisClient gets redis client
func RedisClient() *redis.ClusterClient {

	host := os.Getenv("FEEDS_REDIS_CLUSTER_HOST")
	if len(host) == 0 {

		panic("missing feeds redis host")
	}

	auth := strings.TrimSpace(os.Getenv("FEEDS_REDIS_CLUSTER_PASSWORD"))
	username := strings.TrimSpace(os.Getenv("FEEDS_REDIS_CLUSTER_USERNAME"))

	if len(username) == 0 {

		username = "default"
	}

	addr := strings.Split(host, ",")

	dsn := fmt.Sprintf("redis://%s:%s@%s:6379?dial_timeout=3&read_timeout=6s", username, auth, host)

	opts, _ := redis.ParseClusterURL(dsn)

	/*
		opts := redis.ClusterOptions{
			MinIdleConns: 10,
			//IdleTimeout:  60 * time.Second,
			PoolSize:    10000,
			Addrs:       addr,
			ReadTimeout: 3 * time.Second,
			Username:    username,
			// To route commands by latency or randomly, enable one of the following.
			//RouteByLatency: true,
			//RouteRandomly: true,
		}
	*/

	if len(auth) > 0 {

		// opts.Password = auth
	}

	client := redis.NewClusterClient(opts)

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

		log.Printf("Failed to ping redis | %v | username %s | auth %s | %s", addr, username, auth, err.Error())
		panic(err)
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
