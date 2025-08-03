package feeds

import (
	"context"
	"github.com/touchvas/odds-sdk/v3/models"
)

type Feed interface {
	// OddsChange Updates new odds change message
	OddsChange(ctx context.Context, odds models.OddsChange) (int, error)

	// BetStop Updates new bet stop message
	BetStop(ctx context.Context, producerID, matchID, status int64, statusName string, betradarTimeStamp, publishTimestamp, publisherProcessingTime, networkLatency int64) error

	// GetAllMarkets Gets all markets for a specified matchID
	GetAllMarkets(ctx context.Context, producerID, matchID int64) []models.Market

	// GetMarket Gets only markets for the supplied matchID and specifier
	GetMarket(ctx context.Context, producerID, matchID, marketID int64, specifier string) *models.Market

	// GetOdds Gets Odds for the specified outcome specified by matchID, marketID, specifier, outcomeID
	GetOdds(ctx context.Context, matchID, marketID int64, specifier, outcomeID string) *models.OddsDetails

	// GetAllMarketsOrderByList Gets all markets for a specified matchID order by the supplied ordered list
	GetAllMarketsOrderByList(ctx context.Context, producerID, matchID int64, marketOderList []models.MarketOrderList) []models.Market

	// GetSpecifiedMarkets Gets all markets for a specified matchID only retrieve markets in the supplied list
	GetSpecifiedMarkets(ctx context.Context, producerID, matchID int64, marketList []models.MarketOrderList) []models.Market

	// DeleteAllMarkets Deletes all odds and caches for the supplied match
	DeleteAllMarkets(ctx context.Context, producerID, matchID int64) error

	// DeleteAll Deletes all odds
	DeleteAll(ctx context.Context) error

	// SetProducerID Sets ProducerID for the specified match
	SetProducerID(ctx context.Context, matchID, producerID int64) error

	// GetProducerID Get ProducerID for the specified match
	GetProducerID(ctx context.Context, matchID int64) int64

	// DeleteMatchOdds Delete all odds and caches for the supplied match
	DeleteMatchOdds(ctx context.Context, matchID int64)

	// GetDefaultMarketID Get the default marketID for the specified sportID
	GetDefaultMarketID(ctx context.Context, matchID, sportID int64) int64

	// GetFixtureStatus gets fixture status for the supplied matchID
	GetFixtureStatus(ctx context.Context, matchID int64) *models.FixtureStatus

	// SetFixtureStatus sets fixture status for the supplied matchID
	SetFixtureStatus(ctx context.Context, matchID int64, fx models.FixtureStatus) error
}
