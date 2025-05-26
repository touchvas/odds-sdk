package mysqlfeeds

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	goutils "github.com/mudphilo/go-utils"
	"github.com/nats-io/nats.go"
	"github.com/touchvas/odds-sdk/v2/constants/sport_event_status"
	"github.com/touchvas/odds-sdk/v2/feeds"
	"github.com/touchvas/odds-sdk/v2/models"
	"github.com/touchvas/odds-sdk/v2/utils"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Feed struct {
	feeds.Feed
	DB          *sql.DB
	NatsClient  *nats.Conn
	RedisClient *redis.Client
}

type marketTmp struct {

	// MarketURL market url
	MarketURL string `json:"market_url"  validate:"required"`

	//MarketName market name
	MarketName string `json:"market_name"  validate:"required"`

	//MarketID market ID
	MarketID int64 `json:"market_id"  validate:"required"`

	//Specifier market line
	Specifier string `json:"specifiers"  validate:"required"`

	//StatusName market status name
	StatusName string `json:"status_name"`

	//Status market status 0 (active) else suspended. Suspended odds should be greyed in the UI
	Status int64 `json:"status"  validate:"required"`

	//OutcomeName outcome name
	OutcomeName string `json:"outcome_name"  validate:"required"`

	//OutcomeID outcome ID
	OutcomeID string `json:"outcome_id"  validate:"required"`

	//Odds odds for this particular selection
	Odds float64 `json:"odds"  validate:"required"`

	//Active when 1 (active) display the odds else dont shw the odds on the UI
	Active int64 `json:"active"  validate:"required"`

	//Probability odds probability
	Probability float64 `json:"probability"  validate:"required"`
}

var instance *Feed
var once sync.Once

func GetFeedsInstance() *Feed {

	once.Do(func() {

		fmt.Println("Creating Redis Feeds instance")
		instance = &Feed{
			DB:          DbInstance(),
			NatsClient:  utils.GetNatsConnection(),
			RedisClient: utils.RedisClient(),
		}
	})

	return instance

}

// OddsChange Update new odds change message
func (rds *Feed) OddsChange(odds models.OddsChange) (int, error) {

	DebugMatchID, _ := strconv.ParseInt(os.Getenv("DEBUG_MATCH_ID"), 10, 64)

	//log.Printf("Odds Change | %d | markets %d | producerID %d ", odds.MatchID, len(odds.Markets), odds.ProducerID)

	defaultMarketID := int64(0)

	uniqueTotalMarkets := make(map[int64]int64)

	defaultMarketsList := []string{"1", "186", "219", "340", "251"}

	if odds.MatchID == DebugMatchID {

		log.Printf("%d received markets %d ", odds.MatchID, len(odds.Markets))

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

	dbUtils := goutils.Db{DB: rds.DB, Context: context.TODO()}

	matchDetails := make(map[string]interface{})

	matchDetails["match_id"] = odds.MatchID
	matchDetails["sport_id"] = odds.SportID
	matchDetails["producer_id"] = odds.ProducerID

	table := "live_odds"

	if odds.ProducerID == 3 {

		table = "odds"

	}

	// loop through all the received markets
	for _, m := range odds.Markets {

		// only update market status if there are no outcomes
		if len(m.Outcomes) == 0 {

			updates := map[string]interface{}{
				"status":      m.Status,
				"status_name": m.StatusName,
			}

			condition := map[string]interface{}{
				"sport_id":  odds.SportID,
				"match_id":  odds.MatchID,
				"market_id": m.MarketID,
				"specifier": m.Specifier,
			}

			_, err := dbUtils.UpdateWithContext(table, condition, updates)
			if err != nil {

				log.Printf("error updating odds %s ", err.Error())
			}

			continue
		}

		if defaultMarketID == 0 {

			if goutils.Contains(defaultMarketsList, fmt.Sprintf("%d", m.MarketID)) {

				defaultMarketID = m.MarketID

			}

		}

		// update odds
		for _, o := range m.Outcomes {

			inserts := map[string]interface{}{
				"sport_id":     odds.SportID,
				"match_id":     odds.MatchID,
				"market_id":    m.MarketID,
				"market_name":  m.MarketName,
				"specifier":    m.Specifier,
				"status":       m.Status,
				"status_name":  m.StatusName,
				"outcome_id":   o.OutcomeID,
				"outcome_name": o.OutcomeName,
				"odds":         o.Odds,
				"active":       o.Active,
				"probability":  o.Probability,
			}

			_, err := dbUtils.UpsertWithContext(table, inserts, []string{"status", "status_name", "odds", "probability", "active"})
			if err != nil {

				log.Printf("error updating odds %s ", err.Error())
			}

		}

		if m.Status == 0 || m.Status == 5 {

			uniqueTotalMarkets[m.MarketID] = 1
		}

	}

	if defaultMarketID > 0 {

		matchDetails["default_market"] = defaultMarketID

	}

	ttl := time.Now().UnixMilli() - odds.BetradarTimestamp

	processingTime := time.Now().UnixMilli() - odds.ConsumerArrivalTime

	mq := odds.ConsumerArrivalTime - odds.PublishTimestamp
	publisher := odds.ProcessingTime

	if ttl > 2000 {

		// if this logs appears too frequently then we have an issue,
		// @TODO send slack alerts if code gets here more than 5 times in one minute, this means processing of feeds is slow
		log.Printf("Producer %d | OddsChange | %dms | processing %dms | queueing time %dms | time to publisher %dms ", odds.ProducerID, ttl, processingTime, mq, publisher)

	}

	return 0, nil

}

func (rds *Feed) SetProducerID(matchID, producerID int64) error {

	dbUtils := goutils.Db{DB: rds.DB, Context: context.TODO()}
	updates := map[string]interface{}{
		"match_id":    matchID,
		"producer_id": producerID,
	}

	_, err := dbUtils.UpsertWithContext("match_odds_details", updates, []string{"producer_id"})
	if err != nil {

		log.Printf("error updating match_odds_details %s ", err.Error())
	}

	return err
}

// GetProducerID gets the active producer for a particular match
func (rds *Feed) GetProducerID(matchID int64) (id, status int64) {

	dbUtils := goutils.Db{DB: rds.DB, Context: context.TODO()}
	query := "SELECT m.producer_id, p.producer_status " +
		" FROM match_odds_details m " +
		" INNER JOIN producer p ON m.producer_id = p.producer_id " +
		" WHERE m.match_id = ? "

	dbUtils.SetQuery(query)
	dbUtils.SetParams(matchID)

	var producerID, producerStatus sql.NullInt64

	err := dbUtils.FetchOneWithContext().Scan(&producerID, &producerStatus)
	if err != nil {

		log.Printf("error getting producer status %s ", err.Error())

	}

	return producerID.Int64, producerStatus.Int64

}

// BetStop process bet stop message, this message suspends all the markets
// the markets will be openned up again by subsequent odds change message
func (rds *Feed) BetStop(producerID, matchID, status int64, statusName string, betradarTimeStamp, publishTimestamp, publisherProcessingTime, networkLatency int64) error {

	arrival := time.Now().UnixMilli()

	dbUtils := goutils.Db{DB: rds.DB, Context: context.TODO()}

	table := "live_odds"

	if producerID == 3 {

		table = "odds"

	}

	query := fmt.Sprintf("UPDATE %s SET status = ?, status_name = ?  WHERE match_id = ? ", table)
	dbUtils.SetQuery(query)
	dbUtils.SetParams(status, statusName, matchID)

	_, err := dbUtils.UpdateQueryWithContext()
	if err != nil {

		log.Printf("error processing bet stop %s ", err.Error())
	}

	updates := map[string]interface{}{
		"producer_id":   producerID,
		"active_market": 0,
	}

	condition := map[string]interface{}{
		"match_id": matchID,
	}

	_, err = dbUtils.UpdateWithContext("match_odds_details", condition, updates)
	if err != nil {

		log.Printf("error processing updating match_odds_details %s ", err.Error())
	}

	// log time taken to process odds, we have to process within 2s

	ttl := time.Now().UnixMilli() - betradarTimeStamp

	processingTime := time.Now().UnixMilli() - arrival
	mq := arrival - publishTimestamp
	publisher := publisherProcessingTime

	if ttl > 2000 {

		// if this logs appears too frequently then we have an issue,
		// @TODO send slack alerts if code gets here more than 5 times in one minute, this means processing of feeds is slow
		log.Printf("Producer %d | BetStop | %dms | processing %dms | waiting %dms | publisher ttl %dms | latency %dms", producerID, ttl, processingTime, mq, publisher, networkLatency)

	}

	return nil
}

// GetAllMarkets gets all markets with odds for a particular matchID
func (rds *Feed) GetAllMarkets(producerID, matchID int64) []models.Market {

	dbUtils := goutils.Db{DB: rds.DB, Context: context.TODO()}

	table := "live_odds"

	if producerID == 3 {

		table = "odds"

	}

	query := fmt.Sprintf("SELECT market_id, market_name,status_name, specifier, outcome_name, outcome_id, odds, probability, status, active"+
		" FROM %s WHERE match_id = ?", table)

	dbUtils.SetQuery(query)
	dbUtils.SetParams(matchID)

	rows, err := dbUtils.FetchWithContext()
	if err != nil && err == sql.ErrNoRows {

		rds.RequestOdds(matchID)
		return nil
	}

	if err != nil {

		log.Printf("error getting odds for matchID %d | %s ", matchID, err.Error())
		return nil
	}

	defer rows.Close()

	out := make(map[string][]marketTmp)

	for rows.Next() {

		var market_name, specifier, outcome_name, outcome_id, status_name sql.NullString
		var market_id, status, active sql.NullInt64
		var odds, probability sql.NullFloat64

		err = rows.Scan(&market_id, &market_name, &status_name, &specifier, &outcome_name, &outcome_id, &odds, &probability, &status, &active)
		if err != nil {

			log.Printf("error scanning odds from %s | %s ", table, err.Error())
			continue
		}

		marketKey := fmt.Sprintf("%d:%s", market_id.Int64, specifier.String)

		var outcomes []marketTmp

		if outC, ok := out[marketKey]; !ok {

			outcomes = outC
		}

		outcomes = append(outcomes, marketTmp{
			MarketID:    market_id.Int64,
			Specifier:   specifier.String,
			StatusName:  status_name.String,
			Status:      status.Int64,
			MarketName:  market_name.String,
			OutcomeName: outcome_name.String,
			OutcomeID:   outcome_id.String,
			Odds:        odds.Float64,
			Active:      active.Int64,
			Probability: probability.Float64,
		})

		out[marketKey] = outcomes

	}

	dt := make(map[string]models.Market)

	for k, v := range out {

		var outcomes []models.Outcome

		marketID := int64(0)
		marketName := ""
		specifier := ""
		status := int64(0)
		statusName := ""

		// generate outcomes
		for _, v1 := range v {

			marketID = v1.MarketID
			marketName = v1.MarketName
			specifier = v1.Specifier
			status = v1.Status
			statusName = v1.StatusName

			outcomes = append(outcomes, models.Outcome{
				OutcomeName: v1.OutcomeName,
				OutcomeID:   v1.OutcomeID,
				Odds:        v1.Odds,
				Active:      v1.Active,
				Probability: v1.Probability,
			})
		}

		dt[k] = models.Market{
			MarketName: marketName,
			MarketID:   marketID,
			Specifier:  specifier,
			StatusName: statusName,
			Status:     status,
			Outcomes:   outcomes,
		}
	}

	var markets []models.Market

	for _, v := range dt {

		markets = append(markets, v)

	}

	return markets
}

// GetMarket gets market with odds for a particular matchID and marketID
func (rds *Feed) GetMarket(producerID, matchID, marketID int64, specifier string) *models.Market {

	dbUtils := goutils.Db{DB: rds.DB, Context: context.TODO()}

	table := "live_odds"

	if producerID == 3 {

		table = "odds"

	}

	query := fmt.Sprintf("SELECT market_id, market_name,status_name, specifier, outcome_name, outcome_id, odds, probability, status, active"+
		" FROM %s WHERE match_id = ? AND market_id = ? AND specifier = ? ", table)

	dbUtils.SetQuery(query)
	dbUtils.SetParams(matchID, marketID, specifier)

	rows, err := dbUtils.FetchWithContext()
	if err != nil && err == sql.ErrNoRows {

		rds.RequestOdds(matchID)
		return nil
	}

	var outcomes []models.Outcome

	marketId := int64(0)
	marketName := ""
	status := int64(0)
	statusName := ""

	for rows.Next() {

		var market_name, specifierV, outcome_name, outcome_id, status_name sql.NullString
		var market_id, statusV, active sql.NullInt64
		var odds, probability sql.NullFloat64

		err = rows.Scan(&market_id, &market_name, &status_name, &specifierV, &outcome_name, &outcome_id, &odds, &probability, &statusV, &active)
		if err != nil {

			log.Printf("error scanning odds from %s | %s ", table, err.Error())
			continue
		}

		outcomes = append(outcomes, models.Outcome{
			OutcomeName: outcome_name.String,
			OutcomeID:   outcome_id.String,
			Odds:        odds.Float64,
			Active:      active.Int64,
			Probability: probability.Float64,
		})

		marketId = market_id.Int64
		marketName = market_name.String
		status = statusV.Int64
		statusName = status_name.String
	}

	market := models.Market{
		MarketName: marketName,
		MarketID:   marketId,
		Specifier:  specifier,
		StatusName: statusName,
		Status:     status,
		Outcomes:   outcomes,
	}

	return &market
}

// GetOdds gets odds from quadruplets matchID, marketID , specifier and outcomeID
func (rds *Feed) GetOdds(matchID, marketID int64, specifier, outcomeID string) *models.OddsDetails {

	producerID, _ := rds.GetProducerID(matchID)

	dbUtils := goutils.Db{DB: rds.DB, Context: context.TODO()}

	table := "live_odds"

	if producerID == 3 {

		table = "odds"

	}

	query := fmt.Sprintf("SELECT sport_id,market_id, market_name,status_name, specifier, outcome_name, outcome_id, odds, probability, status, active"+
		" FROM %s WHERE match_id = ? AND market_id = ? AND specifier = ? AND outcome_id = ? ", table)

	dbUtils.SetQuery(query)
	dbUtils.SetParams(matchID, marketID, specifier, outcomeID)

	var market_name, specifierV, outcome_name, outcome_id, status_name sql.NullString
	var sport_id, market_id, statusV, active sql.NullInt64
	var odds, probability sql.NullFloat64

	err := dbUtils.FetchOneWithContext().Scan(&sport_id, &market_id, &market_name, &status_name, &specifierV, &outcome_name, &outcome_id, &odds, &probability, &statusV, &active)
	if err != nil && err == sql.ErrNoRows {

		rds.RequestOdds(matchID)
		return nil
	}

	if err != nil {

		log.Printf("error scanning rows for odds %s ", err.Error())
		return nil
	}

	oddT := models.OddsDetails{
		SportID:     sport_id.Int64,
		MatchID:     matchID,
		MarketID:    marketID,
		MarketName:  market_name.String,
		Specifier:   specifier,
		OutcomeID:   outcomeID,
		OutcomeName: outcome_name.String,
		Status:      statusV.Int64,
		Active:      active.Int64,
		StatusName:  status_name.String,
		Odds:        odds.Float64,
		Probability: probability.Float64,
		Event:       "match",
		EventType:   "match",
		EventPrefix: "sr",
		ProducerID:  producerID,
	}

	return &oddT
}

func (rds *Feed) RequestOdds(matchID int64) error {

	return utils.PublishToNats(rds.NatsClient, "odds_recovery", map[string]interface{}{"match_id": matchID})

}

// GetAllMarketsOrderByList gets all markets with odds for a particular matchID order by the supplied list of markets
func (rds *Feed) GetAllMarketsOrderByList(producerID, matchID int64, marketOderList []models.MarketOrderList) []models.Market {

	markets := rds.GetAllMarkets(producerID, matchID)

	var orderedMarkets, marketsInTheOrderedList, otherMarkets []models.Market

	var marketIDs []string

	for _, k := range marketOderList {

		marketIDs = append(marketIDs, fmt.Sprintf("%d", k.MarketID))

	}

	for _, m := range markets {

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
func (rds *Feed) GetSpecifiedMarkets(producerID, matchID int64, marketList []models.MarketOrderList) []models.Market {

	markets := rds.GetAllMarkets(producerID, matchID)

	var orderedMarkets, marketsInTheOrderedList []models.Market

	var marketIDs []string

	for _, k := range marketList {

		marketIDs = append(marketIDs, fmt.Sprintf("%d", k.MarketID))

	}

	for _, m := range markets {

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
func (rds *Feed) DeleteAllMarkets(producerID, matchID int64) error {

	dbUtils := goutils.Db{DB: rds.DB, Context: context.TODO()}

	table := "live_odds"

	if producerID == 3 {

		table = "odds"

	}

	condition := map[string]interface{}{
		"match_id": matchID,
	}

	_, err := dbUtils.DeleteWithContext(table, condition)
	if err != nil {

		log.Printf("error deleting data from %s %s ", table, err.Error())

	}

	_, err = dbUtils.DeleteWithContext("match_odds_details", condition)
	if err != nil {

		log.Printf("error deleting data from match_odds_details %s ", err.Error())

	}

	return err
}

// DeleteAll deletes all feeds data
func (rds *Feed) DeleteAll() error {

	dbUtils := goutils.Db{DB: rds.DB, Context: context.TODO()}

	tables := []string{"odds", "live_odds", "match_odds_details"}

	for _, t := range tables {

		dbUtils.SetQuery(fmt.Sprintf("TRUNCATE TABLE %s", t))
		_, err := dbUtils.UpdateQueryWithContextTx()
		if err != nil {

			log.Printf("error truncating table %s %s ", t, err.Error())

		}

	}

	return nil

}

// DeleteMatchOdds Delete all odds and caches for the supplied match
func (rds *Feed) DeleteMatchOdds(matchID int64) {

	var keysPattern []string

	rds.DeleteAllMarkets(1, matchID)
	rds.DeleteAllMarkets(3, matchID)

	stasKey := fmt.Sprintf("fixture-stats:%d", matchID)
	keysPattern = append(keysPattern, stasKey)

	matchPriorityKey := fmt.Sprintf("match-priority:%d", matchID)
	keysPattern = append(keysPattern, matchPriorityKey)

	// delete match date
	matchDateKeys := fmt.Sprintf("match-date:%d", matchID)
	keysPattern = append(keysPattern, matchDateKeys)

	sportsKey := fmt.Sprintf("sport-id:%d", matchID)
	keysPattern = append(keysPattern, sportsKey)

	for _, key := range keysPattern {

		if strings.Contains(key, "*") {

			utils.DeleteKeysByPattern(rds.RedisClient, key)

		} else {

			utils.DeleteRedisKey(rds.RedisClient, key)

		}
	}

}

// GetDefaultMarketID gets the default marketID for a particular sportID
func (rds *Feed) GetDefaultMarketID(matchID, sportID int64) int64 {

	dbUtils := goutils.Db{DB: rds.DB, Context: context.TODO()}
	dbUtils.SetQuery("SELECT default_market FROM match_odds_details WHERE match_id = ? ")
	dbUtils.SetParams(matchID)

	var marketID sql.NullInt64

	err := dbUtils.FetchOneWithContext().Scan(&marketID)
	if err != nil && err != sql.ErrNoRows {

		log.Printf("error getting default_market %s ", err.Error())
		return 0
	}

	if marketID.Int64 > 0 {

		return marketID.Int64

	}

	if sportID == 1 {

		return 1
	}

	return 186
}

func (rds *Feed) GetProducerStatus(producerID int64) int64 {

	dbUtils := goutils.Db{DB: rds.DB, Context: context.TODO()}
	query := "SELECT producer_status " +
		" FROM producer " +
		" WHERE producer_id = ? "

	dbUtils.SetQuery(query)
	dbUtils.SetParams(producerID)

	var producerStatus sql.NullInt64

	err := dbUtils.FetchOneWithContext().Scan(producerStatus)
	if err != nil {

		log.Printf("error getting producer status %s ", err.Error())

	}

	return producerStatus.Int64

}

// GetFixtureStatus gets fixture status for the supplied matchID
func (rds *Feed) GetFixtureStatus(matchID int64) models.FixtureStatus {

	market := new(models.FixtureStatus)

	redisKey := fmt.Sprintf("fixture-stats:%d", matchID)

	data, _ := utils.GetRedisKey(rds.RedisClient, redisKey)
	if len(data) == 0 {

		rds.RequestMatchTime(matchID)

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

// SetFixtureStatus sets fixture status for the supplied matchID
func (rds *Feed) SetFixtureStatus(matchID int64, fx models.FixtureStatus) error {

	redisKey := fmt.Sprintf("fixture-stats:%d", matchID)

	js, _ := json.Marshal(fx)

	err := utils.SetRedisKey(rds.RedisClient, redisKey, string(js))
	if err != nil {

		log.Printf("error setting redis key %s | %s", redisKey, err.Error())
	}

	return err

}

func (rds *Feed) RequestMatchTime(matchID int64) error {

	return utils.PublishToNats(rds.NatsClient, "match_timeline", map[string]interface{}{"match_id": matchID})

}
