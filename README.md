# Odds SDK

Odds SDK is a library to extend feeds to other services via a library

## _Configurations_

Setup the below environment variables for this library to work

| Variable                   | Description                                             |
|----------------------------|---------------------------------------------------------|
| ODDS_REDIS_HOST            | Redis host for odds service                             |
| ODDS_REDIS_PORT            | Redis port for odds service                             |
| ODDS_REDIS_DATABASE_NUMBER | Redis index for odds service                            |
| ODDS_REDIS_PASSWORD        | Redis password for odds service, leave black if no auth |
| ODDS_FEED_NAMESPACE        | Namespace of odds service                               |
| DEBUG_MATCH_ID             | Is set a debug log will be output for the set matchID   |

### library installation

```shell
go get -u github.com/touchvas/odds-sdk/v3
```

### library usage
You have to get instance of redisfeed with

```go

package testfeed

import (
	feeds "github.com/touchvas/odds-sdk/v3/feeds"
)

feeds := feeds.GetFeedsInstance()

```

Available functions

| Methods                  | Description                                                                             |
|--------------------------|-----------------------------------------------------------------------------------------|
| OddsChange               | Update new odds change message                                                          |
| BetStop                  | Update new bet stop message                                                             |
| GetAllMarkets            | Gets all markets for a specified matchID                                                |
| GetMarket                | Get only markets for the supplied matchID and specifier                                 |
| GetOdds                  | Get Odds for the specified outcome specified by matchID, marketID, specifier, outcomeID |
| GetAllMarketsOrderByList | Gets all markets for a specified matchID order by the supplied ordered list             |
| GetSpecifiedMarkets      | Gets all markets for a specified matchID only retrieve markets in the supplied list     |
| DeleteAllMarkets         | Delete all odds and caches for the supplied match                                       |
| DeleteAll                | Deletes all odds                                                                        |
| SetProducerID            | Sets ProducerID for the specified match                                                 |
| GetProducerID            | Get ProducerID for the specified match                                                  |
| DeleteMatchOdds          | Delete all odds and caches for the supplied match                                       |
| GetDefaultMarketID       | Get the default marketID for the specified sportID                                      |
