package models

type OddsChange struct {
	ConsumerArrivalTime int64         `json:"consumer_arrival_time"`
	ProducerStatus      int64         `json:"producer_status"`
	PublishTimestamp    int64         `json:"publish_timestamp"`
	BetradarTimestamp   int64         `json:"betradar_timestamp"`
	ProcessingTime      int64         `json:"processing_time"`
	ReceivedTimestamp   int64         `json:"received_timestamp"`
	NetworkLatency      int64         `json:"network_latency"`
	Event               string        `json:"event"`
	EventID             int64         `json:"event_id"`
	EventType           string        `json:"event_type"`
	EventPrefix         string        `json:"event_prefix"`
	MatchID             int64         `json:"match_id"`
	ProducerName        string        `json:"producer_name"`
	ProducerID          int64         `json:"producer_id"`
	SportID             int64         `json:"sport_id"`
	StatusName          string        `json:"status_name"`
	Status              int64         `json:"status"`
	Markets             []Market      `json:"markets"`
	FixtureStatus       FixtureStatus `json:"fixture_status"`
}

type FixtureStatus struct {
	Status           int64  `json:"status"`
	StatusName       string `json:"status_name"`
	HomeScore        string `json:"home_score"`
	AwayScore        string `json:"away_score"`
	MatchStatus      string `json:"match_status"`
	StatusCode       int64  `json:"status_code"`
	EventTime        string `json:"event_time"`
	HomePenaltyScore int64  `json:"home_penalty_score"`
	AwayPenaltyScore int64  `json:"away_penalty_score"`
	ActiveMarkets    int64  `json:"markets"`
}

type Outcome struct {

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

type Market struct {

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

	//Outcomes market outcomes
	Outcomes []Outcome `json:"outcome"`
}

type OddsDetails struct {
	SportID     int64   `json:"sport_id"`
	MatchID     int64   `json:"match_id"`
	MarketID    int64   `json:"market_id"`
	MarketName  string  `json:"market_name"`
	Specifier   string  `json:"specifier"`
	OutcomeID   string  `json:"outcome_id"`
	OutcomeName string  `json:"outcome_name"`
	Status      int64   `json:"status"`
	Active      int64   `json:"active"`
	StatusName  string  `json:"status_name"`
	Odds        float64 `json:"odds"`
	Probability float64 `json:"probability"`
	Event       string  `json:"event"`
	EventType   string  `json:"event_type"`
	ProducerID  int64   `json:"producer_id"`
}

type MarketOrderList struct {
	MarketID   int64
	MarketName string
}
