package spotfeed

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// ErrMissingData is the error returned if there is no data for the query time period.
var ErrMissingData = fmt.Errorf("missing data")

// Period is a time period with a start and end time.
type Period struct {
	Start, End time.Time
}

type Cost struct {
	// Period defines the time period for which this cost is applicable.
	Period
	// ChargeUSD is the total charge over the time period specified by Period.
	ChargeUSD float64
}

// Querier provides the ability to query for costs.
type Querier interface {

	// Query computes the cost charged for the given instanceId for given time period
	// assuming that terminated was the time at which the instance was terminated.
	//
	// It is not required to specify terminated time.  Specifying it only impacts cost
	// calculations for a time period that overlaps the last partial hour of the instance's lifetime.
	//
	// For example, if the instance was running only for say 30m in the last partial hour, and if the
	// desired time period overlaps say the first 15m of that hour, then one must specify
	// terminated time to compute the cost correctly.  In this example, not specifying terminated time
	// would result in a cost higher than actual (ie for the entire last 30 mins instead of only 15 mins).
	//
	// If the given time period spans beyond the instance's actual lifetime, the returned cost will
	// yet only reflect the lifetime cost.  While the returned cost will have the correct start time,
	// the correct end time will be set only if terminated time is provided.
	//
	// Query will return ErrMissingData if it has no data for the given instanceId or
	// if it doesn't have data overlapping the given time period.
	Query(instanceId string, p Period, terminated time.Time) (Cost, error)
}

// NewQuerier fetches data from the given loader and returns a Querier based on the returned data.
func NewQuerier(ctx context.Context, l Loader) (Querier, error) {
	entries, err := l.Fetch(ctx, false)
	if err != nil {
		return nil, err
	}
	return newQuerier(entries), nil
}

// querier provides a Querier implementation using static list of entries provided upon initialization.
type querier struct {
	byInstanceId map[string][]*Entry
}

func newQuerier(all []*Entry) *querier {
	byInstanceId := make(map[string][]*Entry)
	for _, entry := range all {
		iid := entry.InstanceID
		if _, ok := byInstanceId[iid]; !ok {
			byInstanceId[iid] = []*Entry{}
		}
		byInstanceId[iid] = append(byInstanceId[iid], entry)
	}
	for iid, entries := range byInstanceId {
		// For each instance, first sort all the entries
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Timestamp.Before(entries[j].Timestamp)
		})
		var (
			prev       *Entry
			iidEntries []*Entry
		)
		// There can be multiple entries at the same Timestamp, one for each Version.
		// We simply take the entry of the version that has the max cost for the same timestamp.
		for _, entry := range entries {
			switch {
			case prev == nil:
			case prev.Timestamp != entry.Timestamp:
				iidEntries = append(iidEntries, prev)
			case entry.ChargeUSD > prev.ChargeUSD:
			default:
				continue // Keep prev as-is.
			}
			prev = entry
		}
		if prev != nil {
			iidEntries = append(iidEntries, prev)
		}
		byInstanceId[iid] = iidEntries
	}
	return &querier{byInstanceId: byInstanceId}
}

// Query implements Querier interface.
func (q *querier) Query(instanceId string, p Period, terminated time.Time) (Cost, error) {
	p.Start, p.End = p.Start.Truncate(time.Second), p.End.Truncate(time.Second)
	entries, ok := q.byInstanceId[instanceId]
	if !ok || len(entries) == 0 {
		return Cost{}, ErrMissingData
	}
	i := sort.Search(len(entries), func(i int) bool {
		// This will return an entry after p.Start, even if one exists exactly at p.Start
		return p.Start.Before(entries[i].Timestamp)
	})
	switch {
	case i == len(entries):
		// Start is past all entries, so we don't have any data for the given time period.
		return Cost{}, ErrMissingData
	case i == 0 && entries[i].Timestamp.After(p.End):
		// End is before the first entry, so we don't have any data for the given time period.
		return Cost{}, ErrMissingData
	case i > 0:
		// Since we always get the entry after p.Start, we have to move back (if possible)
		// to cover the time period starting from p.Start.
		i--
	}
	var (
		ended bool
		cost  = Cost{Period: Period{End: p.End}}
		prev  = entries[i]
	)
	if startTs := entries[i].Timestamp; p.Start.After(startTs) {
		cost.Start = p.Start
	} else {
		cost.Start = startTs
	}
	for i++; !ended && i < len(entries); i++ {
		startTs := prev.Timestamp
		endTs := entries[i].Timestamp
		if p.Start.After(startTs) {
			startTs = p.Start
		}
		if p.End.Before(endTs) {
			ended = true
			endTs = p.End
		}
		ratio := endTs.Sub(startTs).Seconds() / entries[i].Timestamp.Sub(prev.Timestamp).Seconds()
		cost.ChargeUSD += ratio * prev.ChargeUSD
		prev = entries[i]
	}
	if !ended {
		ratio := 1.0
		switch {
		case terminated.IsZero():
		case p.End.Before(terminated):
			ratio = p.End.Sub(prev.Timestamp).Seconds() / terminated.Sub(prev.Timestamp).Seconds()
		default:
			cost.End = terminated
		}
		cost.ChargeUSD += ratio * prev.ChargeUSD
	}
	return cost, nil
}
