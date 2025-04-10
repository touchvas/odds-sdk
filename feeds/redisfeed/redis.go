package redisfeed

import (
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	goutils "github.com/mudphilo/go-utils"
	"github.com/touchvas/odds-sdk/constants"
	"github.com/touchvas/odds-sdk/feeds"
	"github.com/touchvas/odds-sdk/models"
	"github.com/touchvas/odds-sdk/utils"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var NameSpace = os.Getenv("ODDS_FEED_NAMESPACE")

type RedisFeed struct {
	feeds.Feed
	RedisClient *redis.Client
}

var instance *RedisFeed
var once sync.Once

func GetFeedsInstance() *RedisFeed {

	once.Do(func() {

		fmt.Println("Creating Redis Feeds instance")
		instance = &RedisFeed{
			RedisClient: utils.RedisClient(),
		}
	})

	return instance

}

// OddsChange Update new odds change message
func (rds RedisFeed) OddsChange(odds models.OddsChange) (int, error) {

	DebugMatchID, _ := strconv.ParseInt(os.Getenv("DEBUG_MATCH_ID"), 10, 64)

	log.Printf("Odds Change | %d | markets %d | producerID %d ", odds.MatchID, len(odds.Markets), odds.ProducerID)

	defaultMarketID := int64(0)

	uniqueTotalMarkets := make(map[int64]int64)

	defaultMarketsList := []string{"1", "186", "219", "340", "251"}

	// get table name based on producerID
	tableName := fmt.Sprintf("%s:%s", NameSpace, constants.PreMatchSet)

	if odds.ProducerID == 1 || odds.ProducerID == 4 {

		tableName = fmt.Sprintf("%s:%s", NameSpace, constants.LiveSet)
	}

	// namespace:table:matchID
	keyName := fmt.Sprintf(constants.KeyTemplate, tableName, odds.MatchID)

	if odds.MatchID == DebugMatchID {

		log.Printf("Primary key name %s | received markets %d ", keyName, len(odds.Markets))

	}

	if odds.MatchID == DebugMatchID {

		jsP, _ := json.Marshal(odds)
		log.Printf("%s", string(jsP))

	}

	// odds will come with empty or zero markets if the odds were meant to update match status or match scores
	// this handled by a different process
	if len(odds.Markets) == 0 {

		if odds.MatchID == DebugMatchID {

			log.Printf("no markets for matchID %d ", odds.MatchID)

		}

		return 0, nil
	}

	// get existing data
	// Read a record

	keyExists := rds.keyExist(keyName)

	matchKeys := fmt.Sprintf(constants.KeysFieldTemplate, keyName)

	// set the active producer for this match
	rds.SetProducerID(odds.MatchID, odds.ProducerID)

	// create new keys if it does not exist, this occurs the first time we receive odds for a match
	//or the first odds after a match transitions from prematch to live (producerID changes)
	if !keyExists {

		var keys []string

		if odds.MatchID == DebugMatchID {

			log.Printf("keyExists %s does not exist ", keyName)

		}

		// loop through all the received markets
		for _, m := range odds.Markets {

			cacheValue, _ := json.Marshal(m)

			specifierKey := m.Specifier
			if len(specifierKey) == 0 {

				// if specifier is empty, to avoid using empty in between key name, we use no-specifier
				specifierKey = constants.EmptySpecifier
			}

			// namespace:table:match-matchID:market-marketID:specifierKey
			redisMarketKey := fmt.Sprintf("%s:market-%d:%s", keyName, m.MarketID, specifierKey)
			if odds.MatchID == DebugMatchID {

				log.Printf("Saving data to %s ", redisMarketKey)

			}

			// save each market data as redis keys
			// this will be used on the homepage or when gettings odds via GRPC
			utils.SetRedisKey(rds.RedisClient, redisMarketKey, string(cacheValue))

			// keep a record of all created market keys
			keys = append(keys, redisMarketKey)

			if defaultMarketID == 0 && len(m.Outcomes) > 0 {

				if goutils.Contains(defaultMarketsList, fmt.Sprintf("%d", m.MarketID)) {

					defaultMarketID = m.MarketID

				}

			}

			if len(m.Outcomes) > 0 && (m.Status == 0 || m.Status == 5) {

				uniqueTotalMarkets[m.MarketID] = 1
			}

		}

		// save the entire markets into one key, this will be used in get more/detailed/all market endpoint
		jsonValue, _ := json.Marshal(odds.Markets)
		utils.SetRedisKey(rds.RedisClient, keyName, string(jsonValue))

		// save all the market keys for easier retrieval of data later
		jsonValue, _ = json.Marshal(keys)
		utils.SetRedisKey(rds.RedisClient, matchKeys, string(jsonValue))

		if defaultMarketID > 0 {

			defaultMarketKey := fmt.Sprintf("%s:default-market-id:%d", NameSpace, odds.MatchID)
			utils.SetRedisKey(rds.RedisClient, defaultMarketKey, fmt.Sprintf("%d", defaultMarketID))

		}

		totalMarketsKey := fmt.Sprintf("%s:total-markets:%d", NameSpace, odds.MatchID)
		utils.SetRedisKey(rds.RedisClient, totalMarketsKey, fmt.Sprintf("%d", len(uniqueTotalMarkets)))

		if DebugMatchID == odds.MatchID {

			log.Printf("all keys %s ", string(jsonValue))
		}

		ttl := time.Now().UnixMilli() - odds.ReceivedTimestamp
		processingTime := time.Now().UnixMilli() - odds.ConsumerArrivalTime

		// log time taken to process odds, we have to process within 2s
		mq := odds.ConsumerArrivalTime - odds.PublishTimestamp
		publisher := odds.ProcessingTime

		if ttl > 2000 {

			// if this logs appears too frequently then we have an issue,
			// @TODO send slack alerts if code gets here more than 5 times in one minute, this means processing of feeds is slow
			log.Printf("Producer %d | OddsChange | %s | %dms | processing %dms | queueing time %dms | time to publisher %dms ", odds.ProducerID, keyName, ttl, processingTime, mq, publisher)

		}

		return len(odds.Markets), nil

	}

	// if we get here, there is an already existing markets for this match
	// this feeds may contain all the markets or only a few markets that already exist or new markets that does not exist
	// we have to only update markets we have received in odds.Markets, the other markets should be left unchanged

	var keys []string

	// get all existing market keys for this matchID
	keysListAsString, _ := utils.GetRedisKey(rds.RedisClient, matchKeys)
	err := json.Unmarshal([]byte(keysListAsString), &keys)
	if err != nil {

		log.Printf("failed to unmarshal %s to market keys array []string %s ", keysListAsString, err.Error())

	}

	// to ensure we are not storing duplicate match keys, we have to use a different data structure (map[string]int) to store keys
	uniqueKeys := make(map[string]models.Market)
	for _, k := range keys {

		uniqueKeys[k] = models.Market{
			MarketID: 0, // set marketID as 0 to enable us replace it later
		}
	}

	for _, m := range odds.Markets {

		specifierKey := m.Specifier
		if len(specifierKey) == 0 {

			// if specifier is empty, to avoid using empty in between key name, we use no-specifier
			specifierKey = constants.EmptySpecifier
		}

		// namespace:table:match-matchID:market-marketID:specifierKey
		redisMarketKey := fmt.Sprintf("%s:market-%d:%s", keyName, m.MarketID, specifierKey)
		if odds.MatchID == DebugMatchID {

			log.Printf("saving data to %s ", redisMarketKey)

		}

		// if odds dont have outcome, this message is meant to only update market status of the existing markets
		// this scenario occurs when specific markets have to be suspended e.g first half markets when the 1st half ends
		if len(m.Outcomes) == 0 { // only update market status

			// check if the market exists
			// only update markets that exists

			marketDataAsString, _ := utils.GetRedisKey(rds.RedisClient, redisMarketKey)
			if len(marketDataAsString) > 0 {

				var market models.Market
				err = json.Unmarshal([]byte(marketDataAsString), &market)
				if err != nil {

					log.Printf("error unmarshaling %s to models.Market %s ", marketDataAsString, err.Error())
					continue
				}

				// update market status with new status received
				market.Status = m.Status
				market.StatusName = m.StatusName

				// save market to redis
				jsonValue, _ := json.Marshal(market)
				utils.SetRedisKey(rds.RedisClient, redisMarketKey, string(jsonValue))

				// save market to keys map
				uniqueKeys[redisMarketKey] = market

			}

			continue
		}

		// replace existing market with new market values we have received
		// create new market if it does not exists
		//currentMarkets[redisMarketKey] = m

		// save market to redis
		jsonValue, _ := json.Marshal(m)
		utils.SetRedisKey(rds.RedisClient, redisMarketKey, string(jsonValue))

		// save market to keys map
		uniqueKeys[redisMarketKey] = m

		if defaultMarketID == 0 && len(m.Outcomes) > 0 {

			if goutils.Contains(defaultMarketsList, fmt.Sprintf("%d", m.MarketID)) {

				defaultMarketID = m.MarketID

			}

		}

		continue

	}

	// all the market keys updated accordingly and persisted in redis
	// lets pull all the data together
	var allMarketsData []models.Market

	var allKeys []string

	for k, v := range uniqueKeys {

		if odds.MatchID == DebugMatchID {

			log.Printf("Unique keys %s ", k)

		}

		if len(v.Outcomes) > 0 && (v.Status == 0 || v.Status == 5) {

			uniqueTotalMarkets[v.MarketID] = 1
		}

		// save keys
		allKeys = append(allKeys, k)

		if v.MarketID > 0 { // market data already exist

			allMarketsData = append(allMarketsData, v)
			continue

		}

		marketDataAsString, _ := utils.GetRedisKey(rds.RedisClient, k)
		if len(marketDataAsString) > 0 {

			var market models.Market
			err = json.Unmarshal([]byte(marketDataAsString), &market)
			if err != nil {

				log.Printf("error unmarshaling %s to models.Market %s ", marketDataAsString, err.Error())
				continue
			}

			allMarketsData = append(allMarketsData, market)

		}

	}

	// lets do market ordering,
	// allMarketsData = rds.orderByPriority(allMarketsData, priorityList1)

	jsonValue, _ := json.Marshal(allMarketsData)
	utils.SetRedisKey(rds.RedisClient, keyName, string(jsonValue))
	if DebugMatchID == odds.MatchID {

		log.Printf("all markets %s ", string(jsonValue))
	}

	jsonValue, _ = json.Marshal(allKeys)
	utils.SetRedisKey(rds.RedisClient, matchKeys, string(jsonValue))

	if DebugMatchID == odds.MatchID {

		log.Printf("all keys %s ", string(jsonValue))
	}

	if defaultMarketID > 0 {

		defaultMarketKey := fmt.Sprintf("%s:default-market-id:%d", NameSpace, odds.MatchID)
		utils.SetRedisKey(rds.RedisClient, defaultMarketKey, fmt.Sprintf("%d", defaultMarketID))

	}

	totalMarketsKey := fmt.Sprintf("%s:total-markets:%d", NameSpace, odds.MatchID)
	utils.SetRedisKey(rds.RedisClient, totalMarketsKey, fmt.Sprintf("%d", len(uniqueTotalMarkets)))

	ttl := time.Now().UnixMilli() - odds.ReceivedTimestamp

	processingTime := time.Now().UnixMilli() - odds.ConsumerArrivalTime

	mq := odds.ConsumerArrivalTime - odds.PublishTimestamp
	publisher := odds.ProcessingTime

	if ttl > 2000 {

		// if this logs appears too frequently then we have an issue,
		// @TODO send slack alerts if code gets here more than 5 times in one minute, this means processing of feeds is slow
		log.Printf("Producer %d | OddsChange | %s | %dms | processing %dms | queueing time %dms | time to publisher %dms ", odds.ProducerID, keyName, ttl, processingTime, mq, publisher)

	}

	return len(allMarketsData), nil

}

// BetStop process bet stop message, this message suspends all the markets
// the markets will be openned up again by subsequent odds change message
func (rds RedisFeed) BetStop(producerID, matchID, status int64, statusName string, betradarTimeStamp, publishTimestamp, publisherProcessingTime, networkLatency int64) error {

	arrival := time.Now().UnixMilli()

	log.Printf("Bet Stop | %d | producerID %d ", matchID, producerID)

	// get table name based on producerID
	tableName := fmt.Sprintf("%s:%s", NameSpace, constants.PreMatchSet)

	if producerID == 1 || producerID == 4 {

		tableName = fmt.Sprintf("%s:%s", NameSpace, constants.LiveSet)
	}

	// namespace:table:matchID
	keyName := fmt.Sprintf(constants.KeyTemplate, tableName, matchID)

	keyExists := rds.keyExist(keyName)
	if !keyExists {

		log.Printf("got bet stop for a match that does not exist in our record - %s ", keyName)
		return fmt.Errorf("got bet stop for a match that does not exist in our record - %s ", keyName)
	}

	matchData := new([]models.Market)

	// get all markets for this matchID and suspend them
	matchDataAsString, _ := utils.GetRedisKey(rds.RedisClient, keyName)
	err := json.Unmarshal([]byte(matchDataAsString), matchData)
	if err != nil {

		log.Printf("BetStop - failed to unmarshall %s to JSON %s", matchDataAsString, err.Error())
		return err
	}

	// set the active producer for this match
	rds.SetProducerID(matchID, producerID)

	markets := *matchData

	// loop through each market and update the status with the status received from betstop
	for i, m := range markets {

		m.Status = status
		m.StatusName = statusName
		markets[i] = m

		specifierKey := m.Specifier
		if len(specifierKey) == 0 {

			specifierKey = constants.EmptySpecifier
		}

		// update data saved under market keys with updated data
		// namespace:table:match-matchID:market-marketID:specifierKey
		redisMarketKey := fmt.Sprintf("%s:market-%d:%s", keyName, m.MarketID, specifierKey)

		// replace existing data
		jsonValue, _ := json.Marshal(m)
		utils.SetRedisKey(rds.RedisClient, redisMarketKey, string(jsonValue))

	}

	// replace existing data
	jsonValue, _ := json.Marshal(markets)
	utils.SetRedisKey(rds.RedisClient, keyName, string(jsonValue))

	// log time taken to process odds, we have to process within 2s

	ttl := time.Now().UnixMilli() - betradarTimeStamp

	processingTime := time.Now().UnixMilli() - arrival
	mq := arrival - publishTimestamp
	publisher := publisherProcessingTime

	if ttl > 2000 {

		// if this logs appears too frequently then we have an issue,
		// @TODO send slack alerts if code gets here more than 5 times in one minute, this means processing of feeds is slow
		log.Printf("Producer %d | BetStop | %s | %dms | processing %dms | waiting %dms | publisher ttl %dms | latency %dms", producerID, keyName, ttl, processingTime, mq, publisher, networkLatency)

	}

	return nil
}

// GetAllMarkets gets all markets with odds for a particular matchID
func (rds RedisFeed) GetAllMarkets(producerID, matchID int64) []models.Market {

	// get table name based on producerID
	tableName := fmt.Sprintf("%s:%s", NameSpace, constants.PreMatchSet)

	if producerID == 1 || producerID == 4 {

		tableName = fmt.Sprintf("%s:%s", NameSpace, constants.LiveSet)
	}

	// namespace:table:matchID
	keyName := fmt.Sprintf(constants.KeyTemplate, tableName, matchID)

	keyExists := rds.keyExist(keyName)

	if !keyExists {

		log.Printf("got bet stop for a match that does not exist in our record - %s ", keyName)
		return nil
	}

	markets := new([]models.Market)

	matchDataAsString, _ := utils.GetRedisKey(rds.RedisClient, keyName)
	err := json.Unmarshal([]byte(matchDataAsString), markets)
	if err != nil {

		log.Printf("GetAllMarkets failed to unmarshall %s to JSON %s", matchDataAsString, err.Error())
		return nil
	}

	return *markets
}

// GetMarket gets market with odds for a particular matchID and marketID
func (rds RedisFeed) GetMarket(producerID, matchID, marketID int64, specifier string) *models.Market {

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
	keyExists := rds.keyExist(redisMarketKey)
	if !keyExists {

		log.Printf("key %s does not exists", redisMarketKey)
		return nil
	}

	market := new(models.Market)

	matchDataAsString, _ := utils.GetRedisKey(rds.RedisClient, redisMarketKey)
	err := json.Unmarshal([]byte(matchDataAsString), market)
	if err != nil {

		log.Printf("%s | GetMarket failed to unmarshall %s to JSON %s", redisMarketKey, matchDataAsString, err.Error())
		return nil
	}

	return market

}

// GetOdds gets odds from quadruplets matchID, marketID , specifier and outcomeID
func (rds RedisFeed) GetOdds(matchID, marketID int64, specifier, outcomeID string) *models.OddsDetails {

	// get table name based on producerID
	tableName := fmt.Sprintf("%s:%s", NameSpace, constants.PreMatchSet)

	producerID := rds.GetProducerID(matchID)

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

	// get existing data
	// Read a record
	keyExists := rds.keyExist(redisMarketKey)
	if !keyExists {

		allMarkets := rds.GetAllMarkets(producerID, matchID)

		for _, k := range allMarkets {

			if k.MarketID == marketID && k.Specifier == specifier {

				for _, v := range k.Outcomes {

					if v.OutcomeID == outcomeID {

						return &models.OddsDetails{
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
							Event:       "match",
							ProducerID:  producerID,
							Probability: v.Probability,
						}
					}
				}

			}

		}

		log.Printf("key %s does not exists", redisMarketKey)
		return nil
	}

	market := new(models.Market)

	matchDataAsString, _ := utils.GetRedisKey(rds.RedisClient, redisMarketKey)
	err := json.Unmarshal([]byte(matchDataAsString), market)
	if err != nil {

		log.Printf("GetOdds - failed to unmarshall %s to JSON %s", matchDataAsString, err.Error())
		return nil
	}

	// loop through to get matching outcomes
	for _, v := range market.Outcomes {

		if v.OutcomeID == outcomeID {

			return &models.OddsDetails{
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
				Event:       "match",
				ProducerID:  producerID,
				Probability: v.Probability,
			}
		}
	}

	return nil
}

// GetAllMarketsOrderByList gets all markets with odds for a particular matchID order by the supplied list of markets
func (rds RedisFeed) GetAllMarketsOrderByList(producerID, matchID int64, marketOderList []models.MarketOrderList) []models.Market {

	// get table name based on producerID
	tableName := fmt.Sprintf("%s:%s", NameSpace, constants.PreMatchSet)

	if producerID == 1 || producerID == 4 {

		tableName = fmt.Sprintf("%s:%s", NameSpace, constants.LiveSet)
	}

	// namespace:table:matchID
	keyName := fmt.Sprintf(constants.KeyTemplate, tableName, matchID)

	keyExists := rds.keyExist(keyName)

	if !keyExists {

		log.Printf("got bet stop for a match that does not exist in our record - %s ", keyName)
		return nil
	}

	markets := new([]models.Market)
	var orderedMarkets, marketsInTheOrderedList, otherMarkets []models.Market

	matchDataAsString, _ := utils.GetRedisKey(rds.RedisClient, keyName)
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
func (rds RedisFeed) GetSpecifiedMarkets(producerID, matchID int64, marketList []models.MarketOrderList) []models.Market {

	// get table name based on producerID
	tableName := fmt.Sprintf("%s:%s", NameSpace, constants.PreMatchSet)

	if producerID == 1 || producerID == 4 {

		tableName = fmt.Sprintf("%s:%s", NameSpace, constants.LiveSet)
	}

	// namespace:table:matchID
	keyName := fmt.Sprintf(constants.KeyTemplate, tableName, matchID)

	keyExists := rds.keyExist(keyName)

	if !keyExists {

		log.Printf("GetSpecifiedMarkets - %s key does not exist", keyName)
		return nil
	}

	markets := new([]models.Market)
	var orderedMarkets, marketsInTheOrderedList []models.Market

	matchDataAsString, _ := utils.GetRedisKey(rds.RedisClient, keyName)
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
func (rds RedisFeed) DeleteAllMarkets(producerID, matchID int64) error {

	// get table name based on producerID
	tableName := fmt.Sprintf("%s:%s", NameSpace, constants.PreMatchSet)

	if producerID == 1 || producerID == 4 {

		tableName = fmt.Sprintf("%s:%s", NameSpace, constants.LiveSet)
	}

	// namespace:table:matchID
	keyName := fmt.Sprintf(constants.KeyTemplate, tableName, matchID)

	keyExists := rds.keyExist(keyName)

	if !keyExists {

		log.Printf("got bet stop for a match that does not exist in our record - %s ", keyName)
		return nil
	}

	utils.DeleteRedisKey(rds.RedisClient, keyName)
	utils.DeleteKeysByPattern(rds.RedisClient, fmt.Sprintf("%s:*", keyName))

	return nil
}

// DeleteAll deletes all feeds data
func (rds RedisFeed) DeleteAll() error {

	return utils.DeleteKeysByPattern(rds.RedisClient, "feeds:*")

}

func (rds RedisFeed) SetProducerID(matchID, producerID int64) error {

	redisKey := fmt.Sprintf(constants.ProducerTemplate, matchID)
	return utils.SetRedisKey(rds.RedisClient, redisKey, fmt.Sprintf("%d", producerID))

}

// gets the active producer for a particular match
func (rds RedisFeed) GetProducerID(matchID int64) int64 {

	redisKey := fmt.Sprintf(constants.ProducerTemplate, matchID)
	producer, _ := utils.GetRedisKey(rds.RedisClient, redisKey)
	producerID, _ := strconv.ParseInt(producer, 10, 64)
	return producerID

}

func (rds RedisFeed) keyExist(key string) bool {

	check, err := rds.RedisClient.Exists(key).Result()
	if err != nil {

		log.Printf("error saving redisKey %s error %s", key, err.Error())
		return false
	}

	return check > 0
}

func (rds RedisFeed) getAllKeysByPattern(keyPattern string) []string {

	var keys []string
	iter := rds.RedisClient.Scan(0, keyPattern, 0).Iterator()
	for iter.Next() {

		keys = append(keys, iter.Val())
	}

	return keys
}

func (rds RedisFeed) getAllMarketsOrderByPriority(producerID, matchID int64, marketOderList []models.MarketOrderList) []models.Market {

	DebugMatchID, _ := strconv.ParseInt(os.Getenv("DEBUG_MATCH_ID"), 10, 64)

	// get table name based on producerID
	tableName := fmt.Sprintf("%s:%s", NameSpace, constants.PreMatchSet)

	if producerID == 1 || producerID == 4 {

		tableName = fmt.Sprintf("%s:%s", NameSpace, constants.LiveSet)
	}

	// namespace:table:matchID
	keyName := fmt.Sprintf(constants.KeyTemplate, tableName, matchID)

	keyExists := rds.keyExist(keyName)

	if !keyExists {

		log.Printf("got getAllMarketsOrderByPriority for a match that does not exist - %s ", keyName)
		return nil
	}

	// get all redis keys (market keys) attached to this matchID
	var keys []string
	keysData, _ := utils.GetRedisKey(rds.RedisClient, fmt.Sprintf(constants.KeysFieldTemplate, keyName))
	if DebugMatchID == matchID {

		log.Printf("got market keys %s ", keysData)

	}

	log.Printf("market keys for matchID %d | %d keys", matchID, len(keys))

	err := json.Unmarshal([]byte(keysData), &keys)
	if err != nil {

		log.Printf("getAllMarketsOrderByPriority failed to unmarshall %s to JSON %s", keysData, err.Error())
		return nil

	}

	var markets, orderedMarkets, marketsInTheOrderedList, otherMarkets []models.Market

	// get value for each keys gotten
	for _, key := range keys {

		matchDataAsString, _ := utils.GetRedisKey(rds.RedisClient, key)

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

func (rds RedisFeed) orderByPriority(markets []models.Market, marketOderList []models.MarketOrderList) []models.Market {

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

// DeleteMatchOdds Delete all odds and caches for the supplied match
func (rds RedisFeed) DeleteMatchOdds(matchID int64) {

	var keysPattern []string

	keysPattern = append(keysPattern, fmt.Sprintf("%s:%s", NameSpace, constants.PreMatchSet))

	// get table name based on producerID
	tableNamePrematch := fmt.Sprintf("%s:%s", NameSpace, constants.PreMatchSet)
	tableNameLive := fmt.Sprintf("%s:%s", NameSpace, constants.PreMatchSet)

	// odds object
	keyNameLive := fmt.Sprintf(constants.KeyTemplate, tableNameLive, matchID)
	keyNamePrematch := fmt.Sprintf(constants.KeyTemplate, tableNamePrematch, matchID)

	keysPattern = append(keysPattern, keyNameLive)
	keysPattern = append(keysPattern, keyNamePrematch)

	// individual markets
	redisMarketKeyLive := fmt.Sprintf("%s:*", keyNameLive)
	keysPattern = append(keysPattern, redisMarketKeyLive)

	redisMarketPrematch := fmt.Sprintf("%s:*", keyNamePrematch)
	keysPattern = append(keysPattern, redisMarketPrematch)

	// producer
	producerKey := fmt.Sprintf(constants.ProducerTemplate, matchID)
	keysPattern = append(keysPattern, producerKey)

	// fields key
	fieldsKeyLive := fmt.Sprintf(constants.KeysFieldTemplate, fmt.Sprintf(constants.KeyTemplate, tableNameLive, matchID))
	keysPattern = append(keysPattern, fieldsKeyLive)

	fieldsKeyPrematch := fmt.Sprintf(constants.KeysFieldTemplate, fmt.Sprintf(constants.KeyTemplate, tableNamePrematch, matchID))
	keysPattern = append(keysPattern, fieldsKeyPrematch)

	// default market keys
	defaultMarketKey := fmt.Sprintf("%s:default-market-id:%d", NameSpace, matchID)
	keysPattern = append(keysPattern, defaultMarketKey)

	// total market keys
	totalMarketsKey := fmt.Sprintf("%s:total-markets:%d", NameSpace, matchID)
	keysPattern = append(keysPattern, totalMarketsKey)

	stasKey := fmt.Sprintf("fixture-stats:%d", matchID)
	keysPattern = append(keysPattern, stasKey)

	marketsCountPrematch := fmt.Sprintf("fixture:market-count:%s:%d", "prematch_markets", matchID)
	keysPattern = append(keysPattern, marketsCountPrematch)

	marketsCountLive := fmt.Sprintf("fixture:market-count:%s:%d", "live_markets", matchID)
	keysPattern = append(keysPattern, marketsCountLive)

	matchPriorityKey := fmt.Sprintf("match-priority:%d", matchID)
	keysPattern = append(keysPattern, matchPriorityKey)

	for _, key := range keysPattern {

		if strings.Contains(key, "*") {

			utils.DeleteKeysByPattern(rds.RedisClient, key)

		} else {

			utils.DeleteRedisKey(rds.RedisClient, key)

		}
	}

}

// GetDefaultMarketID gets the default marketID for a particular sportID
func (rds RedisFeed) GetDefaultMarketID(matchID, sportID int64) int64 {

	defaultMarketKey := fmt.Sprintf("%s:default-market-id:%d", NameSpace, matchID)
	redisValue, _ := utils.GetRedisKey(rds.RedisClient, defaultMarketKey)
	market, _ := strconv.ParseInt(redisValue, 10, 64)
	if market > 0 {

		return market
	}

	if sportID == 1 {

		return 1
	}

	return 186
}
