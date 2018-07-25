package spotfeed

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/grailbio/base/errors"
)

const (
	feedFileTimestampFormat = "2006-01-02-15"
)

var (
	feedFileNamePattern = regexp.MustCompile(`^[0-9]{12}\.[0-9]{4}(\-[0-9]{2}){3}\.[0-9]{3}.[a-z0-9]{8}(\.gz)?$`)
)

type fileMeta struct {
	filterable

	Name      string
	AccountId string
	Timestamp time.Time
	Version   int64
	IsGzip    bool
}

func (f *fileMeta) accountId() string {
	return f.AccountId
}

func (f *fileMeta) timestamp() time.Time {
	return f.Timestamp
}

func (f *fileMeta) version() int64 {
	return f.Version
}

func parseFeedFileName(name string) (*fileMeta, error) {
	if !feedFileNamePattern.MatchString(name) {
		return nil, fmt.Errorf("%s does not match feed fileMeta pattern, skipping", name)
	}

	fields := strings.Split(name, ".")
	var isGzip bool
	switch len(fields) {
	case 4:
		isGzip = false
	case 5:
		if fields[4] == "gz" {
			isGzip = true
		} else {
			return nil, fmt.Errorf("failed to parse fileMeta name in data feed directory: %s", name)
		}
	default:
		return nil, fmt.Errorf("failed to parse fileMeta name in data feed directory: %s", name)
	}

	timestamp, err := time.Parse(feedFileTimestampFormat, fields[1])
	if err != nil {
		return nil, errors.E(err, fmt.Sprintf("failed to parse timestamp for name %s", name))
	}

	version, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil {
		return nil, errors.E(err, fmt.Sprintf("failed to parse version for name %s", name))
	}

	return &fileMeta{
		Name:      name,
		AccountId: fields[0],
		Timestamp: timestamp,
		Version:   version,
		IsGzip:    isGzip,
	}, nil
}

// Entry corresponds to a single line in a Spot Instance data feed file. The
// Spot Instance data feed files are tab-delimited. Each line in the data file
// corresponds to one instance hour and contains the fields listed in the
// following table. The AccountId field is not specified for each individual entry
// but is given as a prefix in the name of the spot data feed file.
type Entry struct {
	filterable

	// AccountId is a 12-digit account number (ID) that specifies the AWS account
	// billed for this spot instance-hour.
	AccountId string

	// Timestamp is used to determine the price charged for this instance usage.
	// It is not at the hour boundary but within the hour specified by the title of
	// the data feed file that contains this Entry.
	Timestamp time.Time

	// UsageType is the type of usage and instance type being charged for. For
	// m1.small Spot Instances, this field is set to SpotUsage. For all other
	// instance types, this field is set to SpotUsage:{instance-type}. For
	// example, SpotUsage:c1.medium.
	UsageType string

	// Instance is the instance type being charged for and is a member of the
	// set of information provided by UsageType.
	Instance string

	// Operation is the product being charged for. For Linux Spot Instances,
	// this field is set to RunInstances. For Windows Spot Instances, this
	// field is set to RunInstances:0002. Spot usage is grouped according
	// to Availability Zone.
	Operation string

	// InstanceID is the ID of the Spot Instance that generated this instance
	// usage.
	InstanceID string

	// MyBidID is the ID for the Spot Instance request that generated this instance usage.
	MyBidID string

	// MyMaxPriceUSD is the maximum price specified for this Spot Instance request.
	MyMaxPriceUSD float64

	// MarketPriceUSD is the Spot price at the time specified in the Timestamp field.
	MarketPriceUSD float64

	// ChargeUSD is the price charged for this instance usage.
	ChargeUSD float64

	// Version is the version included in the data feed file name for this record.
	Version int64
}

func (e *Entry) accountId() string {
	return e.AccountId
}

func (e *Entry) timestamp() time.Time {
	return e.Timestamp
}

func (e *Entry) version() int64 {
	return e.Version
}

// parsePriceUSD parses a price in USD formatted like "6.669 USD".
func parsePriceUSD(priceField string) (float64, error) {
	trimCurrency := strings.TrimSuffix(priceField, " USD")
	if len(trimCurrency) != (len(priceField) - 4) {
		return 0, fmt.Errorf("failed to trim currency from %s", priceField)
	}
	return strconv.ParseFloat(trimCurrency, 64)
}

// parseUsageType parses the EC2 instance type from the spot data feed column UsageType, as per the AWS documentation.
// For m1.small Spot Instances, this field is set to SpotUsage. For all other instance types, this field is set to
// SpotUsage:{instance-type}. For example, SpotUsage:c1.medium.
func parseUsageType(usageType string) (string, error) {
	fields := strings.Split(usageType, ":")
	if len(fields) == 1 {
		return "m1.small", nil
	}
	if len(fields) == 2 {
		return fields[1], nil
	}
	return "", fmt.Errorf("failed to parse instance from UsageType %s", usageType)
}

const (
	feedLineTimestampFormat = "2006-01-02 15:04:05 MST"
)

// parseFeedLine parses an *Entry from a line in a spot data feed file. The content and ordering of the columns
// in this file are documented at https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-data-feeds.html
func parseFeedLine(line string, accountId string) (*Entry, error) {
	fields := strings.Split(line, "\t")
	if len(fields) != 9 {
		return nil, fmt.Errorf("failed to parse line in data feed: %s", line)
	}

	timestamp, err := time.Parse(feedLineTimestampFormat, fields[0])
	if err != nil {
		return nil, errors.E(err, fmt.Sprintf("failed to parse timestamp for line %s", line))
	}

	instance, err := parseUsageType(fields[1])
	if err != nil {
		return nil, errors.E(err, fmt.Sprintf("failed to parse usage type for line %s", line))
	}

	myMaxPriceUSD, err := parsePriceUSD(fields[5])
	if err != nil {
		return nil, errors.E(err, fmt.Sprintf("failed to parse my max price for line %s", line))
	}

	marketPriceUSD, err := parsePriceUSD(fields[6])
	if err != nil {
		return nil, errors.E(err, fmt.Sprintf("failed to parse market price for line %s", line))
	}

	chargeUSD, err := parsePriceUSD(fields[7])
	if err != nil {
		return nil, errors.E(err, fmt.Sprintf("failed to parse charge for line %s", line))
	}

	version, err := strconv.ParseInt(fields[8], 10, 64)
	if err != nil {
		return nil, errors.E(err, fmt.Sprintf("failed to parse version for line %s", line))
	}

	return &Entry{
		AccountId:      accountId,
		Timestamp:      timestamp,
		UsageType:      fields[1],
		Instance:       instance,
		Operation:      fields[2],
		InstanceID:     fields[3],
		MyBidID:        fields[4],
		MyMaxPriceUSD:  myMaxPriceUSD,
		MarketPriceUSD: marketPriceUSD,
		ChargeUSD:      chargeUSD,
		Version:        version,
	}, nil
}

func ParseFeedFile(feed io.Reader, accountId string) ([]*Entry, error) {
	scn := bufio.NewScanner(feed)

	entries := make([]*Entry, 0)
	for scn.Scan() {
		line := scn.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}

		entry, err := parseFeedLine(scn.Text(), accountId)
		if err != nil {
			return nil, errors.E(err, "")
		}

		entries = append(entries, entry)
	}

	return entries, nil
}
