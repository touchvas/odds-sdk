package main

import (
	"github.com/touchvas/odds-sdk/v3/utils"
	"os"
)

func main() {

	os.Setenv("FEEDS_REDIS_CLUSTER_HOST", "109.205.176.2")
	os.Setenv("FEEDS_REDIS_CLUSTER_PASSWORD", "f86sfK3mnL")

	utils.RedisClient()
}
