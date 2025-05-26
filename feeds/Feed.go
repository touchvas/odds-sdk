package feeds

import "github.com/touchvas/odds-sdk/v2/models"

type Feed interface {
	// OddsChange Updates new odds change message
	OddsChange(odds models.OddsChange) (int, error)

	// BetStop Update new bet stop message
	BetStop(producerID, matchID, status int64, statusName string, betradarTimeStamp, publishTimestamp, publisherProcessingTime, networkLatency int64) error

	// GetAllMarkets Gets all markets for a specified matchID
	GetAllMarkets(producerID, matchID int64) []models.Market

	// GetMarket Get only markets for the supplied matchID and specifier
	GetMarket(producerID, matchID, marketID int64, specifier string) *models.Market

	// GetOdds Get Odds for the specified outcome specified by matchID, marketID, specifier, outcomeID
	GetOdds(matchID, marketID int64, specifier, outcomeID string) *models.OddsDetails

	// GetAllMarketsOrderByList Gets all markets for a specified matchID order by the supplied ordered list
	GetAllMarketsOrderByList(producerID, matchID int64, marketOderList []models.MarketOrderList) []models.Market

	// GetSpecifiedMarkets Gets all markets for a specified matchID only retrieve markets in the supplied list
	GetSpecifiedMarkets(producerID, matchID int64, marketList []models.MarketOrderList) []models.Market

	// DeleteAllMarkets Delete all odds and caches for the supplied match
	DeleteAllMarkets(producerID, matchID int64) error

	// DeleteAll Deletes all odds
	DeleteAll() error

	// SetProducerID Sets ProducerID for the specified match
	SetProducerID(matchID, producerID int64) error

	// GetProducerID Get ProducerID for the specified match
	GetProducerID(matchID int64) int64

	// DeleteMatchOdds Delete all odds and caches for the supplied match
	DeleteMatchOdds(matchID int64)

	// GetDefaultMarketID Get the default marketID for the specified sportID
	GetDefaultMarketID(matchID, sportID int64) int64

	// GetFixtureStatus gets fixture status for the supplied matchID
	GetFixtureStatus(matchID int64) *models.FixtureStatus

	// SetFixtureStatus sets fixture status for the supplied matchID
	SetFixtureStatus(matchID int64, fx models.FixtureStatus) error
}
