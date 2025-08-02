package utils

import (
	"fmt"
	"github.com/go-redis/redis"
	"log"
	"os"
	"strconv"
	"time"
)

// RedisClient gets redis client
func RedisClient() *redis.Client {

	host := os.Getenv("FEEDS_REDIS_HOST")
	if len(host) == 0 {

		panic("missing odds redis host")
	}

	portNumber, _ := strconv.ParseInt(os.Getenv("FEEDS_REDIS_PORT"), 10, 64)

	if portNumber == 0 {

		portNumber = 6379
	}

	dbNumber, _ := strconv.ParseInt(os.Getenv("FEEDS_REDIS_DATABASE_NUMBER"), 10, 64)

	auth := os.Getenv("FEEDS_REDIS_PASSWORD")

	uri := fmt.Sprintf("redis://%s:%d", host, portNumber)
	uri = fmt.Sprintf("%s:%d", host, portNumber)

	opts := redis.Options{
		MinIdleConns: 10,
		//IdleTimeout:  60 * time.Second,
		PoolSize:    10000,
		Addr:        uri,
		DB:          int(dbNumber), // use default DB
		ReadTimeout: 3 * time.Second,
	}

	if len(auth) > 0 {

		opts.Password = auth
	}

	client := redis.NewClient(&opts)

	return client
}

// GetRedisKey get saved key from redis
func GetRedisKey(conn *redis.Client, key string) (string, error) {

	var data string
	data, err := conn.Get(key).Result()
	if err != nil {

		//return data, fmt.Errorf("error getting key %s: %v", key, err)
		return "", err
	}

	return data, err

}

// SetRedisKeyWithExpiry saves key to redis with TTL value
func SetRedisKeyWithExpiry(conn *redis.Client, key string, value string, seconds int) error {

	_, err := conn.Set(key, value, time.Second*time.Duration(seconds)).Result()
	if err != nil {

		v := string(value)

		if len(v) > 15 {

			v = v[0:12] + "..."
		}

		log.Printf("error saving redisKey %s error %s", key, err.Error())
		return fmt.Errorf("error setting key %s to %s: %v", key, v, err)
	}

	return err
}

// SetRedisKey saves key to redis without expiry
func SetRedisKey(conn *redis.Client, key string, value string) error {

	_, err := conn.Set(key, value, 0).Result()
	if err != nil {

		v := string(value)

		if len(v) > 15 {

			v = v[0:12] + "..."
		}

		log.Printf("error saving redisKey %s error %s", key, err.Error())
		return fmt.Errorf("error setting key %s to %s: %v", key, v, err)
	}

	return err
}

// DeleteRedisKey deletes a saved redis keys
func DeleteRedisKey(conn *redis.Client, key string) error {

	_, err := conn.Del(key).Result()
	if err != nil {

		log.Printf("error deleting redisKey %s error %s", key, err.Error())
		return fmt.Errorf("error deleting key %s | %s", key, err)
	}

	return err
}

// DeleteKeysByPattern deletes a set of keys matching the supplied pattern
func DeleteKeysByPattern(conn *redis.Client, keyPattern string) error {

	iter := conn.Scan(0, keyPattern, 0).Iterator()
	for iter.Next() {

		DeleteRedisKey(conn, iter.Val())
	}

	if err := iter.Err(); err != nil {

		log.Printf("error iteration error deleteing keys %s | %s", keyPattern, err.Error())
		return err
	}

	return nil
}

func RedisKeyExists(conn *redis.Client, key string) (bool, error) {

	check, err := conn.Exists(key).Result()
	if err != nil {

		log.Printf("error saving redisKey %s error %s", key, err.Error())
		return false, err
	}

	return check > 0, nil
}

func GetAllKeysByPattern(conn *redis.Client, keyPattern string) []string {

	var keys []string
	iter := conn.Scan(0, keyPattern, 0).Iterator()
	for iter.Next() {

		keys = append(keys, iter.Val())
	}

	return keys
}
