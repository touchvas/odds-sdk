package redisfeed

import (
	"context"
	"encoding/json"
	"fmt"
	goutils "github.com/mudphilo/go-utils"
	"github.com/redis/go-redis/v9"
	"github.com/touchvas/odds-sdk/v3/constants"
	"github.com/touchvas/odds-sdk/v3/constants/sport_event_status"
	"github.com/touchvas/odds-sdk/v3/feeds"
	"github.com/touchvas/odds-sdk/v3/models"
	"github.com/touchvas/odds-sdk/v3/utils"
	"log"
	"os"
	"strconv"
)

var NameSpace = os.Getenv("ODDS_FEED_NAMESPACE")

type RedisFeed struct {
	feeds.Feed
}

var instance *RedisFeed

func GetFeedsInstance() *RedisFeed {

	instance = &RedisFeed{}

	return instance

}

// GetAllMarkets gets all markets with odds for a particular matchID
func (rds *RedisFeed) GetAllMarkets(ctx context.Context, redisClient *redis.Client, producerID, matchID int64) []models.Market {

	// get table name based on producerID
	tableName := fmt.Sprintf("%s:%s", NameSpace, constants.PreMatchSet)

	if producerID == 1 || producerID == 4 {

		tableName = fmt.Sprintf("%s:%s", NameSpace, constants.LiveSet)
	}

	// namespace:table:matchID
	keyName := fmt.Sprintf(constants.KeyTemplate, tableName, matchID)

	keyExists := rds.keyExist(ctx, redisClient, keyName)

	if !keyExists {

		return nil
	}

	markets := new([]models.Market)

	matchDataAsString, _ := utils.GetRedisKey(ctx, redisClient, keyName)
	err := json.Unmarshal([]byte(matchDataAsString), markets)
	if err != nil {

		log.Printf("GetAllMarkets failed to unmarshall %s to JSON %s", matchDataAsString, err.Error())
		return nil
	}

	return *markets
}

// GetMarket gets market with odds for a particular matchID and marketID
func (rds *RedisFeed) GetMarket(ctx context.Context, redisClient *redis.Client, producerID, matchID, marketID int64, specifier string) *models.Market {

	// get table name based on producerID
	tableName := fmt.Sprintf("%s:%s", NameSpace, constants.PreMatchSet)

	if producerID == 1 || producerID == 4 {

		tableName = fmt.Sprintf("%s:%s", NameSpace, constants.LiveSet)
	}

	// namespace:table:matchID
	keyName := fmt.Sprintf(constants.KeyTemplate, tableName, matchID)

	var specifierKey = specifier

	if len(specifierKey) == 0 {

		specifierKey = constants.EmptySpecifier
	}

	// namespace:table:match-matchID:market-marketID:specifierKey
	redisMarketKey := fmt.Sprintf("%s:market-%d:%s", keyName, marketID, specifierKey)

	// get existing data
	// Read a record
	keyExists := rds.keyExist(ctx, redisClient, redisMarketKey)
	if !keyExists {

		return nil
	}

	market := new(models.Market)

	matchDataAsString, _ := utils.GetRedisKey(ctx, redisClient, redisMarketKey)

	if len(matchDataAsString) == 0 {

		return nil
	}

	err := json.Unmarshal([]byte(matchDataAsString), market)
	if err != nil {

		log.Printf("%s | GetMarket failed to unmarshall %s to JSON %s", redisMarketKey, matchDataAsString, err.Error())
		return nil
	}

	return market

}

// GetOdds gets odds from quadruplets matchID, marketID , specifier and outcomeID
func (rds *RedisFeed) GetOdds(ctx context.Context, redisClient *redis.Client, matchID, marketID int64, specifier, outcomeID string) *models.OddsDetails {

	// get table name based on producerID
	tableName := fmt.Sprintf("%s:%s", NameSpace, constants.PreMatchSet)

	producerID, _ := rds.GetProducerID(ctx, redisClient, matchID)

	if producerID == 1 || producerID == 4 {

		tableName = fmt.Sprintf("%s:%s", NameSpace, constants.LiveSet)
	}

	// namespace:table:matchID
	keyName := fmt.Sprintf(constants.KeyTemplate, tableName, matchID)

	specifierKey := specifier

	if len(specifierKey) == 0 {

		specifierKey = constants.EmptySpecifier
	}

	// namespace:table:match-matchID:market-marketID:specifierKey
	redisMarketKey := fmt.Sprintf("%s:market-%d:%s", keyName, marketID, specifierKey)

	sportsKey := fmt.Sprintf("sport-id:%d", matchID)
	sportIDStr, _ := utils.GetRedisKey(ctx, redisClient, sportsKey)
	sportID, _ := strconv.ParseInt(sportIDStr, 10, 64)

	// get existing data
	// Read a record
	keyExists := rds.keyExist(ctx, redisClient, redisMarketKey)
	if !keyExists {

		allMarkets := rds.GetAllMarkets(ctx, redisClient, producerID, matchID)

		for _, k := range allMarkets {

			if k.MarketID == marketID && k.Specifier == specifier {

				for _, v := range k.Outcomes {

					if v.OutcomeID == outcomeID {

						return &models.OddsDetails{
							SportID:     sportID,
							MatchID:     matchID,
							MarketID:    marketID,
							MarketName:  k.MarketName,
							Specifier:   specifier,
							OutcomeID:   outcomeID,
							OutcomeName: v.OutcomeName,
							Status:      k.Status,
							Active:      v.Active,
							StatusName:  k.StatusName,
							Odds:        v.Odds,
							Event:       fmt.Sprintf("%d", matchID),
							ProducerID:  producerID,
							Probability: v.Probability,
							EventType:   "match",
							EventPrefix: "sr",
						}
					}
				}

			}

		}

		return nil
	}

	market := new(models.Market)

	matchDataAsString, _ := utils.GetRedisKey(ctx, redisClient, redisMarketKey)
	err := json.Unmarshal([]byte(matchDataAsString), market)
	if err != nil {

		log.Printf("GetOdds - failed to unmarshall %s to JSON %s", matchDataAsString, err.Error())
		return nil
	}

	// loop through to get matching outcomes
	for _, v := range market.Outcomes {

		if v.OutcomeID == outcomeID {

			return &models.OddsDetails{
				SportID:     sportID,
				MatchID:     matchID,
				MarketID:    marketID,
				MarketName:  market.MarketName,
				Specifier:   specifier,
				OutcomeID:   outcomeID,
				OutcomeName: v.OutcomeName,
				Status:      market.Status,
				Active:      v.Active,
				StatusName:  market.StatusName,
				Odds:        v.Odds,
				Event:       fmt.Sprintf("%d", matchID),
				ProducerID:  producerID,
				Probability: v.Probability,
				EventType:   "match",
				EventPrefix: "sr",
			}
		}
	}

	return nil
}

// GetAllMarketsOrderByList gets all markets with odds for a particular matchID order by the supplied list of markets
func (rds *RedisFeed) GetAllMarketsOrderByList(ctx context.Context, redisClient *redis.Client, producerID, matchID int64, marketOderList []models.MarketOrderList) []models.Market {

	// get table name based on producerID
	tableName := fmt.Sprintf("%s:%s", NameSpace, constants.PreMatchSet)

	if producerID == 1 || producerID == 4 {

		tableName = fmt.Sprintf("%s:%s", NameSpace, constants.LiveSet)
	}

	// namespace:table:matchID
	keyName := fmt.Sprintf(constants.KeyTemplate, tableName, matchID)

	keyExists := rds.keyExist(ctx, redisClient, keyName)

	if !keyExists {

		return nil
	}

	markets := new([]models.Market)
	var orderedMarkets, marketsInTheOrderedList, otherMarkets []models.Market

	matchDataAsString, _ := utils.GetRedisKey(ctx, redisClient, keyName)
	err := json.Unmarshal([]byte(matchDataAsString), markets)
	if err != nil {

		log.Printf("GetAllMarketsOrderByList failed to unmarshall %s to JSON %s", matchDataAsString, err.Error())
		return nil
	}

	var marketIDs []string

	for _, k := range marketOderList {

		marketIDs = append(marketIDs, fmt.Sprintf("%d", k.MarketID))

	}

	for _, m := range *markets {

		// check if marketID exists in the list
		if goutils.Contains(marketIDs, fmt.Sprintf("%d", m.MarketID)) {

			marketsInTheOrderedList = append(marketsInTheOrderedList, m)

		} else {

			otherMarkets = append(otherMarkets, m)

		}
	}

	for _, v := range marketOderList {

		// get associated market
		for _, m := range marketsInTheOrderedList {

			if v.MarketID == m.MarketID {

				if len(v.MarketName) > 0 {

					// m.MarketName = v.MarketName
				}

				orderedMarkets = append(orderedMarkets, m)

			}
		}

	}

	for _, v := range otherMarkets {

		orderedMarkets = append(orderedMarkets, v)

	}

	return orderedMarkets

}

// GetSpecifiedMarkets gets the specified markets with odds for a particular matchID order by the supplied list of markets
func (rds *RedisFeed) GetSpecifiedMarkets(ctx context.Context, redisClient *redis.Client, producerID, matchID int64, marketList []models.MarketOrderList) []models.Market {

	// get table name based on producerID
	tableName := fmt.Sprintf("%s:%s", NameSpace, constants.PreMatchSet)

	if producerID == 1 || producerID == 4 {

		tableName = fmt.Sprintf("%s:%s", NameSpace, constants.LiveSet)
	}

	// namespace:table:matchID
	keyName := fmt.Sprintf(constants.KeyTemplate, tableName, matchID)

	keyExists := rds.keyExist(ctx, redisClient, keyName)

	if !keyExists {

		return nil
	}

	markets := new([]models.Market)
	var orderedMarkets, marketsInTheOrderedList []models.Market

	matchDataAsString, _ := utils.GetRedisKey(ctx, redisClient, keyName)
	err := json.Unmarshal([]byte(matchDataAsString), markets)
	if err != nil {

		log.Printf("GetSpecifiedMarkets failed to unmarshall %s to JSON %s", matchDataAsString, err.Error())
		return nil
	}

	var marketIDs []string

	for _, k := range marketList {

		marketIDs = append(marketIDs, fmt.Sprintf("%d", k.MarketID))

	}

	for _, m := range *markets {

		// check if marketID exists in the list
		if goutils.Contains(marketIDs, fmt.Sprintf("%d", m.MarketID)) {

			marketsInTheOrderedList = append(marketsInTheOrderedList, m)

		}
	}

	for _, v := range marketList {

		// get associated market
		for _, m := range marketsInTheOrderedList {

			if v.MarketID == m.MarketID {

				if len(v.MarketName) > 0 {

					m.MarketName = v.MarketName
				}

				orderedMarkets = append(orderedMarkets, m)

			}
		}

	}

	return orderedMarkets

}

// DeleteAllMarkets deletes markets for the specified matchID
func (rds *RedisFeed) DeleteAllMarkets(ctx context.Context, redisClient *redis.Client, producerID, matchID int64) error {

	// get table name based on producerID
	tableName := fmt.Sprintf("%s:%s", NameSpace, constants.PreMatchSet)

	if producerID == 1 || producerID == 4 {

		tableName = fmt.Sprintf("%s:%s", NameSpace, constants.LiveSet)
	}

	// namespace:table:matchID
	keyName := fmt.Sprintf(constants.KeyTemplate, tableName, matchID)

	keyExists := rds.keyExist(ctx, redisClient, keyName)

	if !keyExists {

		return nil
	}

	utils.DeleteRedisKey(ctx, redisClient, keyName)
	utils.DeleteKeysByPattern(ctx, redisClient, fmt.Sprintf("%s:*", keyName))

	return nil
}

func (rds *RedisFeed) SetProducerID(ctx context.Context, redisClient *redis.Client, matchID, producerID int64) error {

	redisKey := fmt.Sprintf(constants.ProducerTemplate, matchID)
	return utils.SetRedisKey(ctx, redisClient, redisKey, fmt.Sprintf("%d", producerID))

}

// GetProducerID gets the active producer for a particular match
func (rds *RedisFeed) GetProducerID(ctx context.Context, redisClient *redis.Client, matchID int64) (id, status int64) {

	redisKey := fmt.Sprintf(constants.ProducerTemplate, matchID)
	producer, _ := utils.GetRedisKey(ctx, redisClient, redisKey)
	producerID, _ := strconv.ParseInt(producer, 10, 64)
	return producerID, rds.GetProducerStatus(ctx, redisClient, producerID)

}

func (rds *RedisFeed) keyExist(ctx context.Context, redisClient *redis.Client, key string) bool {

	check, err := utils.RedisKeyExists(ctx, redisClient, key)
	if err != nil {

		return false

	}

	return check
}

func (rds *RedisFeed) getAllKeysByPattern(ctx context.Context, redisClient *redis.Client, keyPattern string) []string {

	return utils.GetAllKeysByPattern(ctx, redisClient, keyPattern)
}

func (rds *RedisFeed) getAllMarketsOrderByPriority(ctx context.Context, redisClient *redis.Client, producerID, matchID int64, marketOderList []models.MarketOrderList) []models.Market {

	DebugMatchID, _ := strconv.ParseInt(os.Getenv("DEBUG_MATCH_ID"), 10, 64)

	// get table name based on producerID
	tableName := fmt.Sprintf("%s:%s", NameSpace, constants.PreMatchSet)

	if producerID == 1 || producerID == 4 {

		tableName = fmt.Sprintf("%s:%s", NameSpace, constants.LiveSet)
	}

	// namespace:table:matchID
	keyName := fmt.Sprintf(constants.KeyTemplate, tableName, matchID)

	keyExists := rds.keyExist(ctx, redisClient, keyName)

	if !keyExists {

		return nil
	}

	// get all redis keys (market keys) attached to this matchID
	var keys []string
	keysData, _ := utils.GetRedisKey(ctx, redisClient, fmt.Sprintf(constants.KeysFieldTemplate, keyName))
	if DebugMatchID == matchID {

		log.Printf("got market keys %s ", keysData)

	}

	err := json.Unmarshal([]byte(keysData), &keys)
	if err != nil {

		log.Printf("getAllMarketsOrderByPriority failed to unmarshall %s to JSON %s", keysData, err.Error())
		return nil

	}

	var markets, orderedMarkets, marketsInTheOrderedList, otherMarkets []models.Market

	// get value for each keys gotten
	for _, key := range keys {

		matchDataAsString, _ := utils.GetRedisKey(ctx, redisClient, key)

		if DebugMatchID == matchID {

			log.Printf("market value %s | %s ", key, matchDataAsString)

		}

		m := new(models.Market)
		err = json.Unmarshal([]byte(matchDataAsString), m)
		if err != nil {

			log.Printf("getAllMarketsOrderByPriority failed to unmarshall %s to JSON %s", matchDataAsString, err.Error())
			continue
		}

		markets = append(markets, *m)

	}

	// order markets based on the supplied market list
	var marketIDs []string

	// get marketIDs in the order list
	for _, k := range marketOderList {

		marketIDs = append(marketIDs, fmt.Sprintf("%d", k.MarketID))

	}

	// pull markets in the array of ordered marketID in separate array
	for _, m := range markets {

		// check if marketID exists in the list
		if goutils.Contains(marketIDs, fmt.Sprintf("%d", m.MarketID)) {

			marketsInTheOrderedList = append(marketsInTheOrderedList, m)

		} else {

			otherMarkets = append(otherMarkets, m)

		}
	}

	// merge array starting with ordered markets
	/*
		for _, v := range marketOderList {

			// get associated market
			for _, m := range marketsInTheOrderedList {

				if v.MarketID == m.MarketID {

					if len(v.MarketName) > 0 {

						m.MarketName = v.MarketName
					}

					orderedMarkets = append(orderedMarkets, m)

				}
			}

		}
	*/

	for _, v := range marketsInTheOrderedList {

		orderedMarkets = append(orderedMarkets, v)

	}

	for _, v := range otherMarkets {

		orderedMarkets = append(orderedMarkets, v)

	}

	return orderedMarkets
}

func (rds *RedisFeed) orderByPriority(markets []models.Market, marketOderList []models.MarketOrderList) []models.Market {

	var orderedMarkets, marketsInTheOrderedList, otherMarkets []models.Market

	// order markets based on the supplied market list
	var marketIDs []string

	// get marketIDs in the order list
	for _, k := range marketOderList {

		marketIDs = append(marketIDs, fmt.Sprintf("%d", k.MarketID))

	}

	// pull markets in the array of ordered marketID in separate array
	for _, m := range markets {

		// check if marketID exists in the list
		if goutils.Contains(marketIDs, fmt.Sprintf("%d", m.MarketID)) {

			marketsInTheOrderedList = append(marketsInTheOrderedList, m)

		} else {

			otherMarkets = append(otherMarkets, m)

		}
	}

	// replace market name with name in the marketpriolist list
	for _, v := range marketOderList {

		for k, m := range marketsInTheOrderedList {

			if v.MarketID == m.MarketID {

				//if len(v.MarketName) > 0 {

				//m.MarketName = v.MarketName
				marketsInTheOrderedList[k] = m
				//}
			}
		}

	}

	// merge array starting with ordered markets
	for _, v := range marketsInTheOrderedList {

		orderedMarkets = append(orderedMarkets, v)

	}

	for _, v := range otherMarkets {

		orderedMarkets = append(orderedMarkets, v)

	}

	return orderedMarkets
}

// GetDefaultMarketID gets the default marketID for a particular sportID
func (rds *RedisFeed) GetDefaultMarketID(ctx context.Context, redisClient *redis.Client, matchID, sportID int64) int64 {

	defaultMarketKey := fmt.Sprintf("%s:default-market-id:%d", NameSpace, matchID)
	redisValue, _ := utils.GetRedisKey(ctx, redisClient, defaultMarketKey)
	market, _ := strconv.ParseInt(redisValue, 10, 64)
	if market > 0 {

		return market
	}

	if sportID == 1 {

		return 1
	}

	return 186
}

func (rds *RedisFeed) GetProducerStatus(ctx context.Context, redisClient *redis.Client, producerID int64) int64 {

	redisKey := fmt.Sprintf("producer:status:%d", producerID)
	dt, _ := utils.GetRedisKey(ctx, redisClient, redisKey)
	producerStatus, _ := strconv.ParseInt(dt, 10, 64)
	return producerStatus

}

// GetFixtureStatus gets fixture status for the supplied matchID
func (rds *RedisFeed) GetFixtureStatus(ctx context.Context, redisClient *redis.Client, matchID int64) models.FixtureStatus {

	market := new(models.FixtureStatus)

	redisKey := fmt.Sprintf("fixture-stats:%d", matchID)

	data, _ := utils.GetRedisKey(ctx, redisClient, redisKey)
	if len(data) == 0 {

		return models.FixtureStatus{
			Status:     0,
			StatusName: sport_event_status.NotStarted,
			StatusCode: 0,
		}
	}

	err := json.Unmarshal([]byte(data), market)
	if err != nil {

		log.Printf("%s | GetFixtureStatus failed to unmarshall %s to JSON %s", redisKey, data, err.Error())
		return models.FixtureStatus{
			Status:     0,
			StatusName: sport_event_status.NotStarted,
			StatusCode: 0,
		}
	}

	return *market

}
