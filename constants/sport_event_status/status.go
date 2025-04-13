package sport_event_status

// NotStarted The match has not started yet. (Alternatively, Betradar has no live coverage of the event, the match has started but we do not know this. The match will then move to closed when Betradar enters the match results)
const NotStarted = "not_started"

// Live The match is live
const Live = "live"

// Ended The match has finished, but results have not been confirmed yet.
const Ended = "ended"

// Closed The match is finished, results confirmed, and no more changes are expected to the results (only for events covered by pre-match producer).
const Closed = "closed"

// Cancelled The sport event (either the actual match, or this Betradar representation of the match) has been cancelled
const Cancelled = "cancelled"

// Delayed The sport event start has been delayed from scheduled start (most often seen for tennis).
const Delayed = "delayed"

// Interrupted The sport event has been temporarily interrupted. Interruption is expected to be just a few minutes.
const Interrupted = "interrupted"

// Suspended The sport event looks to be interrupted for a longer period than a few minutes
const Suspended = "suspended"

// Postponed The sport event has been postponed and will be played at a later date. Typically, if the later date is more than 3 days away. This sport event id will be cancelled and replaced by a new id. If the match is postponed to just one or two days from now, the same sport-id will change its state just before match start.
const Postponed = "postponed"

// Abandoned Used to indicate that Betradar has no live coverage or has lost live coverage but match is still likely ongoing.
const Abandoned = "abandoned"
