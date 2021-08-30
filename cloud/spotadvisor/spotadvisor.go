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
	ZeroToFivePct InterruptRange = iota
	FiveToTenPct
	TenToFifteenPct
	FifteenToTwentyPct
	GreaterThanTwentyPct
)

// These need to be in their own const block to ensure iota starts at 0.
const (
	LessThanFivePct InterruptProbability = iota
	LessThanTenPct
	LessThanFifteenPct
	LessThanTwentyPct
	Any
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
	// key is an EC2 instance type name like "r5a.large"
	InstanceTypes map[string]instanceType `json:"instance_types"`
	// key is an AWS region name like "us-west-2"
	SpotAdvisor map[string]osGroups `json:"spot_advisor"`
}

type aggKey struct {
	ot OsType
	ar AwsRegion
	ip InterruptProbability
}

func (k aggKey) String() string {
	return fmt.Sprintf("{%s, %s, %s}", k.ot, k.ar, k.ip)
}

// OsType should only be used via the pre-defined constants in this package.
type OsType string

// AwsRegion is an AWS region name like "us-west-2".
type AwsRegion string

// InstanceType is an EC2 instance type name like "r5a.large".
type InstanceType string

// InterruptRange is the AWS defined interrupt range for an instance type; it
// should only be used via the pre-defined constants in this package.
type InterruptRange int

func (ir InterruptRange) String() string {
	switch ir {
	case ZeroToFivePct:
		return "O-5%"
	case FiveToTenPct:
		return "5-10%"
	case TenToFifteenPct:
		return "10-15%"
	case FifteenToTwentyPct:
		return "15-20%"
	case GreaterThanTwentyPct:
		return "> 20%"
	default:
		return "invalid interrupt range"
	}
}

// InterruptProbability is an upper bound used to indicate multiple interrupt
// ranges; it should only be used via the pre-defined constants in this package.
type InterruptProbability int

func (ir InterruptProbability) String() string {
	switch ir {
	case LessThanFivePct:
		return "< 5%"
	case LessThanTenPct:
		return "< 10%"
	case LessThanFifteenPct:
		return "< 15%"
	case LessThanTwentyPct:
		return "< 20%"
	case Any:
		return "Any"
	default:
		return "invalid interrupt probability"
	}
}

// SpotAdvisor provides an interface for utilizing spot instance interrupt rate
// data and savings data from AWS.
type SpotAdvisor struct {
	mu sync.RWMutex
	// rawData is the decoded spot advisor json response
	rawData advisorData
	// aggData maps each aggKey to a slice of instance types aggregated by interrupt
	// probability. For example, if aggKey.ip=LessThanTenPct, then the mapped value
	// would contain all instance types which have an interrupt range of
	// LessThanFivePct or FiveToTenPct.
	aggData map[aggKey][]string

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
					log.Printf("error refreshing spot advisor data (will try again later): %s", err)
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
	aggData := make(map[aggKey][]string)
	for r, o := range rawData.SpotAdvisor {
		region := AwsRegion(r)
		// transform the raw data so that the values of aggData will contain just the instances in a given range
		for instance, data := range o.Linux {
			k := aggKey{Linux, region, InterruptProbability(data.RangeIdx)}
			aggData[k] = append(aggData[k], instance)
		}
		for instance, data := range o.Windows {
			k := aggKey{Windows, region, InterruptProbability(data.RangeIdx)}
			aggData[k] = append(aggData[k], instance)
		}

		// aggregate instances by the upper bound interrupt probability of each key
		for i := 1; i <= int(Any); i++ {
			{
				lk := aggKey{Linux, region, InterruptProbability(i)}
				lprevk := aggKey{Linux, region, InterruptProbability(i - 1)}
				aggData[lk] = append(aggData[lk], aggData[lprevk]...)
			}
			{
				wk := aggKey{Windows, region, InterruptProbability(i)}
				wprevk := aggKey{Windows, region, InterruptProbability(i - 1)}
				aggData[wk] = append(aggData[wk], aggData[wprevk]...)
			}
		}
	}
	sa.mu.Lock()
	sa.rawData = rawData
	sa.aggData = aggData
	sa.mu.Unlock()
	return nil
}

// FilterByMaxInterruptProbability returns a subset of the input candidates by
// removing instance types which have a probability of interruption greater than ip.
func (sa *SpotAdvisor) FilterByMaxInterruptProbability(ot OsType, ar AwsRegion, candidates []string, ip InterruptProbability) (filtered []string, err error) {
	if ip == Any {
		// There's a chance we may not have spot advisor data for some instances in
		// the candidates, so just return as is without doing a set difference.
		return candidates, nil
	}
	allowed, err := sa.GetInstancesWithMaxInterruptProbability(ot, ar, ip)
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

// GetInstancesWithMaxInterruptProbability returns the set of spot instance types
// with an interrupt probability less than or equal to ip, with the given OS and region.
func (sa *SpotAdvisor) GetInstancesWithMaxInterruptProbability(ot OsType, region AwsRegion, ip InterruptProbability) (map[string]bool, error) {
	if ip < LessThanFivePct || ip > Any {
		return nil, fmt.Errorf("invalid InterruptProbability: %d", ip)
	}
	k := aggKey{ot, region, ip}
	sa.mu.RLock()
	defer sa.mu.RUnlock()
	ts, ok := sa.aggData[k]
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
