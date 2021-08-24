// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package spotadvisor provides an interface for utilizing spot instance
// interrupt rate data and savings data from AWS.
package spotadvisor

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

var spotAdvisorDataUrl = "https://spot-bid-advisor.s3.amazonaws.com/spot-advisor-data.json"

const (
	// Spot Advisor data is only updated a few times a day, so we just refresh once an hour.
	// Might need to revisit this value if data is updated more frequently in the future.
	defaultRefreshInterval = 1 * time.Hour
	defaultRequestTimeout  = 10 * time.Second

	Linux   = OsType("Linux")
	Windows = OsType("Windows")
)

// These need to be in their own const block to ensure iota starts at 0.
const (
	LessThanFivePct InterruptRange = iota
	LessThanTenPct
	LessThanFifteenPct
	LessThanTwentyPct
	All
)

type interruptRange struct {
	Label string `json:"label"`
	Index int    `json:"index"`
	Dots  int    `json:"dots"`
	Max   int    `json:"max"`
}

type instanceType struct {
	Cores int     `json:"cores"`
	Emr   bool    `json:"emr"`
	RamGb float32 `json:"ram_gb"`
}

type instanceData struct {
	RangeIdx int `json:"r"`
	Savings  int `json:"s"`
}

type osGroups struct {
	Windows map[string]instanceData `json:"Windows"`
	Linux   map[string]instanceData `json:"Linux"`
}

type advisorData struct {
	Ranges []interruptRange `json:"ranges"`
	// key is instance type name like "r5a.large"
	InstanceTypes map[string]instanceType `json:"instance_types"`
	// key is AWS region name like "us-west-2"
	SpotAdvisor map[string]osGroups `json:"spot_advisor"`
}

type key struct {
	ot OsType
	ar AwsRegion
	ir InterruptRange
}

func (k key) String() string {
	return fmt.Sprintf("{%s, %s, %s}", k.ot, k.ar, k.ir)
}

// OsType should only be used via the pre-defined constants in this package.
type OsType string

type AwsRegion string

type InstanceType string

// InterruptRange should only be used via the pre-defined constants in this package.
type InterruptRange int

func (ir InterruptRange) String() string {
	switch ir {
	case LessThanFivePct:
		return "< 5%"
	case LessThanTenPct:
		return "< 10%"
	case LessThanFifteenPct:
		return "< 15%"
	case LessThanTwentyPct:
		return "< 20%"
	case All:
		return "< 100%"
	default:
		return "invalid interrupt range"
	}
}

// SpotAdvisor provides an interface for utilizing spot instance interrupt rate
// data and savings data from AWS.
type SpotAdvisor struct {
	mu sync.RWMutex
	// rawData is decoded spot advisor json response
	rawData advisorData
	// aggByRange maps each key to a slice of instance types aggregated <= interrupt range. For example,
	// if key.ir=LessThanTenPct, the mapped value would contain all instances in LessThanTenPct AND LessThanFivePct
	// e.g. {Linux, "us-west-2", LessThanFivePct} -> ["r5a.large", ...]
	aggByRange map[key][]string

	// TODO: incorporate spot advisor savings data
}

// NewSpotAdvisor initializes and returns a SpotAdvisor instance. If
// initialization fails, a nil SpotAdvisor is returned with an error. The
// underlying data is asynchronously updated, until the done channel is closed.
// Errors during updates are non-fatal and will not prevent future updates.
func NewSpotAdvisor(log *log.Logger, done <-chan struct{}) (*SpotAdvisor, error) {
	sa := SpotAdvisor{}
	// initial load
	if err := sa.refresh(); err != nil {
		return nil, fmt.Errorf("error fetching spot advisor data: %s", err)
	}

	go func() {
		ticker := time.NewTicker(defaultRefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if err := sa.refresh(); err != nil {
					log.Printf("error when refreshing spot advisor data (will try again later): %s", err)
				}
			}
		}
	}()
	return &sa, nil
}

func (sa *SpotAdvisor) refresh() (err error) {
	// fetch
	client := &http.Client{Timeout: defaultRequestTimeout}
	resp, err := client.Get(spotAdvisorDataUrl)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s response StatusCode: %s", spotAdvisorDataUrl, http.StatusText(resp.StatusCode))
	}
	var rawData advisorData
	err = json.NewDecoder(resp.Body).Decode(&rawData)
	if err != nil {
		return err
	}
	err = resp.Body.Close()
	if err != nil {
		return err
	}

	// update internal data structures
	aggByRange := make(map[key][]string)
	for r, o := range rawData.SpotAdvisor {
		region := AwsRegion(r)
		// transform raw data
		for instance, data := range o.Linux {
			k := key{Linux, region, InterruptRange(data.RangeIdx)}
			aggByRange[k] = append(aggByRange[k], instance)
		}
		for instance, data := range o.Windows {
			k := key{Windows, region, InterruptRange(data.RangeIdx)}
			aggByRange[k] = append(aggByRange[k], instance)
		}

		// aggregate
		for i := 1; i <= int(All); i++ {
			{
				lk := key{Linux, region, InterruptRange(i)}
				lprevk := key{Linux, region, InterruptRange(i - 1)}
				aggByRange[lk] = append(aggByRange[lk], aggByRange[lprevk]...)
			}
			{
				wk := key{Windows, region, InterruptRange(i)}
				wprevk := key{Windows, region, InterruptRange(i - 1)}
				aggByRange[wk] = append(aggByRange[wk], aggByRange[wprevk]...)
			}
		}
	}
	sa.mu.Lock()
	sa.rawData = rawData
	sa.aggByRange = aggByRange
	sa.mu.Unlock()
	return nil
}

// FilterByInterruptRange returns a subset of the input candidates by removing
// instance types which don't fall into the given interrupt range.
func (sa *SpotAdvisor) FilterByInterruptRange(ot OsType, ar AwsRegion, candidates []string, ir InterruptRange) (filtered []string, err error) {
	if ir == All {
		// There's a chance we may not have spot advisor data for some instances in
		// the candidates, so just return as is without doing a set difference.
		return candidates, nil
	}
	allowed, err := sa.GetInstancesWithMaxInterruptRange(ot, ar, ir)
	if err != nil {
		return nil, err
	}
	for _, c := range candidates {
		if allowed[c] {
			filtered = append(filtered, c)
		}
	}
	return filtered, nil
}

// GetInstancesWithMaxInterruptRange returns the set of instance types with a
// matching OS and region that fall within the given interrupt range.
func (sa *SpotAdvisor) GetInstancesWithMaxInterruptRange(ot OsType, region AwsRegion, ir InterruptRange) (map[string]bool, error) {
	if ir < LessThanFivePct || ir > All {
		return nil, fmt.Errorf("invalid InterruptRange: %d", ir)
	}
	k := key{ot, region, ir}
	sa.mu.RLock()
	defer sa.mu.RUnlock()
	ts, ok := sa.aggByRange[k]
	if !ok {
		return nil, fmt.Errorf("no spot advisor data for: %s", k)
	}
	tsMap := make(map[string]bool, len(ts))
	for _, t := range ts {
		tsMap[t] = true
	}
	return tsMap, nil
}

// GetInterruptRange returns the interrupt range for the instance type with the
// given OS and region.
func (sa *SpotAdvisor) GetInterruptRange(ot OsType, ar AwsRegion, it InstanceType) (InterruptRange, error) {
	sa.mu.RLock()
	defer sa.mu.RUnlock()
	osg, ok := sa.rawData.SpotAdvisor[string(ar)]
	if !ok {
		return -1, fmt.Errorf("no spot advisor data for: %s", ar)
	}
	var m map[string]instanceData
	switch ot {
	case Linux:
		m = osg.Linux
	case Windows:
		m = osg.Windows
	default:
		return -1, fmt.Errorf("invalid OS: %s", ot)
	}

	d, ok := m[string(it)]
	if !ok {
		return -1, fmt.Errorf("no spot advisor data for %s instance type '%s' in %s", ot, it, ar)
	}
	return InterruptRange(d.RangeIdx), nil
}
