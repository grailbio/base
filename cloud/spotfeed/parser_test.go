package spotfeed

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseFeedFileSuccess(t *testing.T) {
	for _, test := range []struct {
		name            string
		feedBlob        string
		expectedEntries []*Entry
	}{
		{
			name: "empty",
			feedBlob: `#Version: 1.0
#Fields: Timestamp UsageType Operation InstanceID MyBidID MyMaxPrice MarketPrice Charge Version`,
			expectedEntries: []*Entry{},
		},
		{
			name: "one line",
			feedBlob: `#Version: 1.0
#Fields: Timestamp UsageType Operation InstanceID MyBidID MyMaxPrice MarketPrice Charge Version
2020-10-29 19:35:44 UTC	USW2-SpotUsage:c4.large	RunInstances:SV002	i-00028539b6b1de1c0	sir-yzyi9qrm	0.100 USD	0.034 USD	0.001 USD	1`,
			expectedEntries: []*Entry{
				{
					AccountId:      testAccountId,
					Timestamp:      time.Date(2020, 10, 29, 19, 35, 44, 0, time.UTC),
					UsageType:      "USW2-SpotUsage:c4.large",
					Instance:       "c4.large",
					Operation:      "RunInstances:SV002",
					InstanceID:     "i-00028539b6b1de1c0",
					MyBidID:        "sir-yzyi9qrm",
					MyMaxPriceUSD:  0.1,
					MarketPriceUSD: 0.034,
					ChargeUSD:      0.001,
					Version:        1,
				},
			},
		},
		{
			name: "m1.small",
			feedBlob: `#Version: 1.0
#Fields: Timestamp UsageType Operation InstanceID MyBidID MyMaxPrice MarketPrice Charge Version
2020-10-29 19:38:33 UTC	USW2-SpotUsage	RunInstances:SV002	i-0c5a748cea172ba6b	sir-7nigaazp	26.688 USD	8.006 USD	4.546 USD	1`,
			expectedEntries: []*Entry{
				{
					AccountId:      testAccountId,
					Timestamp:      time.Date(2020, 10, 29, 19, 38, 33, 0, time.UTC),
					UsageType:      "USW2-SpotUsage",
					Instance:       "m1.small",
					Operation:      "RunInstances:SV002",
					InstanceID:     "i-0c5a748cea172ba6b",
					MyBidID:        "sir-7nigaazp",
					MyMaxPriceUSD:  26.688,
					MarketPriceUSD: 8.006,
					ChargeUSD:      4.546,
					Version:        1,
				},
			},
		},
		{
			name: "multiple",
			feedBlob: `#Version: 1.0
#Fields: Timestamp UsageType Operation InstanceID MyBidID MyMaxPrice MarketPrice Charge Version
2020-10-29 19:35:44 UTC	USW2-SpotUsage:c4.large	RunInstances:SV002	i-00028539b6b1de1c0	sir-yzyi9qrm	0.100 USD	0.034 USD	0.001 USD	1
2020-10-29 19:35:05 UTC	USW2-SpotUsage:c4.large	RunInstances:SV002	i-0003301e05abdf3c1	sir-5d78aggq	0.100 USD	0.034 USD	0.002 USD	1
2020-10-29 19:35:44 UTC	USW2-SpotUsage:c4.large	RunInstances:SV002	i-0028e565fd6e3d37b	sir-9g7ibe6n	0.100 USD	0.034 USD	0.002 USD	1`,
			expectedEntries: []*Entry{
				{
					AccountId:      testAccountId,
					Timestamp:      time.Date(2020, 10, 29, 19, 35, 44, 0, time.UTC),
					UsageType:      "USW2-SpotUsage:c4.large",
					Instance:       "c4.large",
					Operation:      "RunInstances:SV002",
					InstanceID:     "i-00028539b6b1de1c0",
					MyBidID:        "sir-yzyi9qrm",
					MyMaxPriceUSD:  0.1,
					MarketPriceUSD: 0.034,
					ChargeUSD:      0.001,
					Version:        1,
				},
				{
					AccountId:      testAccountId,
					Timestamp:      time.Date(2020, 10, 29, 19, 35, 05, 0, time.UTC),
					UsageType:      "USW2-SpotUsage:c4.large",
					Instance:       "c4.large",
					Operation:      "RunInstances:SV002",
					InstanceID:     "i-0003301e05abdf3c1",
					MyBidID:        "sir-5d78aggq",
					MyMaxPriceUSD:  0.1,
					MarketPriceUSD: 0.034,
					ChargeUSD:      0.002,
					Version:        1,
				},
				{
					AccountId:      testAccountId,
					Timestamp:      time.Date(2020, 10, 29, 19, 35, 44, 0, time.UTC),
					UsageType:      "USW2-SpotUsage:c4.large",
					Instance:       "c4.large",
					Operation:      "RunInstances:SV002",
					InstanceID:     "i-0028e565fd6e3d37b",
					MyBidID:        "sir-9g7ibe6n",
					MyMaxPriceUSD:  0.1,
					MarketPriceUSD: 0.034,
					ChargeUSD:      0.002,
					Version:        1,
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			entries, err := ParseFeedFile(strings.NewReader(test.feedBlob), testAccountId)
			require.NoError(t, err, "failed to parse feed blob")
			require.Equal(t, test.expectedEntries, entries)
		})
	}
}

func TestParseFeedFileNameSuccess(t *testing.T) {
	for _, test := range []struct {
		name         string
		fileName     string
		expectedMeta *fileMeta
	}{
		{
			"no_gzip",
			testAccountId + ".2021-02-23-04.004.8a6d6bb8",
			&fileMeta{
				nil,
				testAccountId + ".2021-02-23-04.004.8a6d6bb8",
				testAccountId,
				time.Date(2021, 02, 23, 04, 0, 0, 0, time.UTC),
				4,
				false,
			},
		},
		{
			"gzip",
			testAccountId + ".2021-02-23-04.004.8a6d6bb8.gz",
			&fileMeta{
				nil,
				testAccountId + ".2021-02-23-04.004.8a6d6bb8.gz",
				testAccountId,
				time.Date(2021, 02, 23, 04, 0, 0, 0, time.UTC),
				4,
				true,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			meta, err := parseFeedFileName(test.fileName)
			require.NoError(t, err, "failed to parse feed file name")
			require.Equal(t, test.expectedMeta, meta)
		})
	}
}

func TestParseFeedFileNameError(t *testing.T) {
	for _, test := range []struct {
		name        string
		fileName    string
		expectedErr string
	}{
		{
			"fail_regex",
			testAccountId + ".2021-02-23-04.004.8bb8",
			"does not match",
		},
		{
			"invalid_extension",
			testAccountId + ".2021-02-23-04.004.8a6d6bb8.zip",
			"does not match",
		},
		{
			"invalid_date",
			testAccountId + ".2021-13-23-04.004.8a6d6bb8.gz",
			"failed to parse timestamp",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseFeedFileName(test.fileName)
			require.Error(t, err, "unexpected error while parsing feed file name %s but call to parseFeedFileName succeeded", test.fileName)
			require.Contains(t, err.Error(), test.expectedErr)
		})
	}
}
