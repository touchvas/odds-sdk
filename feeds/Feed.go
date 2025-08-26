package feeds

import (
	"context"
	"github.com/redis/go-redis/v9"
	"github.com/touchvas/odds-sdk/v3/models"
)

type Feed interface {

	// GetAllMarkets Gets all markets for a specified matchID
	GetAllMarkets(ctx context.Context, redisClient *redis.Client, producerID, matchID int64) []models.Market

	// GetMarket Gets only markets for the supplied matchID and specifier
	GetMarket(ctx context.Context, redisClient *redis.Client, producerID, matchID, marketID int64, specifier string) *models.Market

	// GetOdds Gets Odds for the specified outcome specified by matchID, marketID, specifier, outcomeID
	GetOdds(ctx context.Context, redisClient *redis.Client, matchID, marketID int64, specifier, outcomeID string) *models.OddsDetails

	// GetAllMarketsOrderByList Gets all markets for a specified matchID order by the supplied ordered list
	GetAllMarketsOrderByList(ctx context.Context, redisClient *redis.Client, producerID, matchID int64, marketOderList []models.MarketOrderList) []models.Market

	// GetSpecifiedMarkets Gets all markets for a specified matchID only retrieve markets in the supplied list
	GetSpecifiedMarkets(ctx context.Context, redisClient *redis.Client, producerID, matchID int64, marketList []models.MarketOrderList) []models.Market

	// GetProducerID Get ProducerID for the specified match
	GetProducerID(ctx context.Context, redisClient *redis.Client, matchID int64) int64

	// GetDefaultMarketID Get the default marketID for the specified sportID
	GetDefaultMarketID(ctx context.Context, redisClient *redis.Client, matchID, sportID int64) int64

	// GetFixtureStatus gets fixture status for the supplied matchID
	GetFixtureStatus(ctx context.Context, redisClient *redis.Client, matchID int64) *models.FixtureStatus
}
