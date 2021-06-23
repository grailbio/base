package spotfeed

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/stretchr/testify/require"
)

const (
	testAccountId = "123456789000"
)

func TestFilter(t *testing.T) {
	ptr := func(t time.Time) *time.Time {
		return &t
	}

	for _, test := range []struct {
		name                 string
		filters              filters
		filterable           filterable
		expectedFilterResult bool
	}{
		{
			"passes_filter",
			filters{
				AccountId: testAccountId,
				StartTime: ptr(time.Date(2020, 01, 01, 01, 00, 00, 00, time.UTC)),
				EndTime:   ptr(time.Date(2020, 02, 01, 01, 00, 00, 00, time.UTC)),
				Version:   1,
			},
			&fileMeta{
				nil,
				testAccountId + ".2021-02-23-04.004.8a6d6bb8",
				testAccountId,
				time.Date(2020, 01, 10, 04, 0, 0, 0, time.UTC),
				1,
				false,
			},
			false,
		},
		{
			"filtered_out_by_meta",
			filters{
				Version: 1,
			},
			&fileMeta{
				nil,
				testAccountId + ".2021-02-23-04.004.8a6d6bb8",
				testAccountId,
				time.Date(2021, 02, 23, 04, 0, 0, 0, time.UTC),
				4,
				false,
			},
			true,
		},
		{
			"filtered_out_by_start_time",
			filters{
				StartTime: ptr(time.Date(2020, 01, 01, 01, 00, 00, 00, time.UTC)),
			},
			&fileMeta{
				nil,
				testAccountId + ".2021-02-23-04.004.8a6d6bb8",
				testAccountId,
				time.Date(2019, 02, 23, 04, 0, 0, 0, time.UTC),
				4,
				false,
			},
			true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expectedFilterResult, test.filters.filter(test.filterable))
		})
	}
}

// gzipBytes takes some bytes and performs compression with compress/gzip.
func gzipBytes(b []byte) ([]byte, error) {
	var buffer bytes.Buffer
	gw := gzip.NewWriter(&buffer)
	if _, err := gw.Write(b); err != nil {
		return nil, err
	}
	if err := gw.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

type mockS3Client struct {
	s3iface.S3API

	getObjectResults map[string]string // path (bucket/key) -> decompressed response contents

	listMu                sync.Mutex
	listObjectPageResults [][]string // [](result path slices)
}

func (m *mockS3Client) ListObjectsV2PagesWithContext(_ aws.Context, input *s3.ListObjectsV2Input, callback func(*s3.ListObjectsV2Output, bool) bool, _ ...request.Option) error {
	m.listMu.Lock()
	defer m.listMu.Unlock()

	if len(m.listObjectPageResults) == 0 {
		return fmt.Errorf("unexpected attempt to list s3 objects at path %s/%s", aws.StringValue(input.Bucket), aws.StringValue(input.Prefix))
	}
	var currKeys []string
	currKeys, m.listObjectPageResults = m.listObjectPageResults[0], m.listObjectPageResults[1:]

	resultObjects := make([]*s3.Object, len(currKeys))
	for i, key := range currKeys {
		resultObjects[i] = &s3.Object{
			Key: aws.String(key),
		}
	}

	callback(&s3.ListObjectsV2Output{Contents: resultObjects}, true)
	return nil
}

func (m *mockS3Client) GetObjectWithContext(_ aws.Context, input *s3.GetObjectInput, _ ...request.Option) (*s3.GetObjectOutput, error) {
	path := fmt.Sprintf("%s/%s", aws.StringValue(input.Bucket), aws.StringValue(input.Key))
	if v, ok := m.getObjectResults[path]; ok {
		// compress the response via gzip
		gzContents, err := gzipBytes([]byte(v))
		if err != nil {
			return nil, fmt.Errorf("failed to compress test response at %s: %s", path, err)
		}
		return &s3.GetObjectOutput{
			// compress the response contents via gzip
			Body: ioutil.NopCloser(bytes.NewReader(gzContents)),
		}, nil
	} else {
		return nil, fmt.Errorf("attempted to get unexpected path %s from mock s3", path)
	}
}

func TestLoaderFetch(t *testing.T) {
	ctx := context.Background()
	devNull := log.New(ioutil.Discard, "", 0)
	for _, test := range []struct {
		name            string
		loader          Loader
		expectedEntries []*Entry
	}{
		{
			"s3_no_source_files",
			NewS3Loader("test-bucket", "", &mockS3Client{
				listObjectPageResults: [][]string{
					{}, // single empty response
				},
			}, devNull, testAccountId, nil, nil, 0),
			[]*Entry{},
		},
		{
			"s3_single_source_file",
			NewS3Loader("test-bucket", "", &mockS3Client{
				listObjectPageResults: [][]string{
					{ // single populated response
						testAccountId + ".2021-02-25-15.002.3eb820a5.gz",
					},
				},
				getObjectResults: map[string]string{
					"test-bucket/" + testAccountId + ".2021-02-25-15.002.3eb820a5.gz": `#Version: 1.0
#Fields: Timestamp UsageType Operation InstanceID MyBidID MyMaxPrice MarketPrice Charge Version
2021-02-20 18:52:50 UTC	USW2-SpotUsage:x1.16xlarge	RunInstances:SV002	i-0053c2917e2afa2f0	sir-yb3gavgp	6.669 USD	2.001 USD	0.073 USD	2
2021-02-20 18:50:58 UTC	USW2-SpotUsage:x1.16xlarge	RunInstances:SV002	i-07eaa4b2bf27c4b75	sir-w13gadim	6.669 USD	2.001 USD	1.741 USD	2`,
				},
			}, devNull, testAccountId, nil, nil, 0),
			[]*Entry{
				{
					AccountId:      testAccountId,
					Timestamp:      time.Date(2021, 02, 20, 18, 52, 50, 0, time.UTC),
					UsageType:      "USW2-SpotUsage:x1.16xlarge",
					Instance:       "x1.16xlarge",
					Operation:      "RunInstances:SV002",
					InstanceID:     "i-0053c2917e2afa2f0",
					MyBidID:        "sir-yb3gavgp",
					MyMaxPriceUSD:  6.669,
					MarketPriceUSD: 2.001,
					ChargeUSD:      0.073,
					Version:        2,
				},
				{
					AccountId:      testAccountId,
					Timestamp:      time.Date(2021, 02, 20, 18, 50, 58, 0, time.UTC),
					UsageType:      "USW2-SpotUsage:x1.16xlarge",
					Instance:       "x1.16xlarge",
					Operation:      "RunInstances:SV002",
					InstanceID:     "i-07eaa4b2bf27c4b75",
					MyBidID:        "sir-w13gadim",
					MyMaxPriceUSD:  6.669,
					MarketPriceUSD: 2.001,
					ChargeUSD:      1.741,
					Version:        2,
				},
			},
		},
		{
			"local_no_source_files",
			NewLocalLoader(
				"testdata/no_source_files",
				devNull, testAccountId, nil, nil, 0),
			[]*Entry{},
		},
		{
			"local_single_source_file",
			NewLocalLoader(
				"testdata/single_source_file",
				devNull, testAccountId, nil, nil, 0),
			[]*Entry{
				{
					AccountId:      testAccountId,
					Timestamp:      time.Date(2021, 02, 20, 18, 52, 50, 0, time.UTC),
					UsageType:      "USW2-SpotUsage:x1.16xlarge",
					Instance:       "x1.16xlarge",
					Operation:      "RunInstances:SV002",
					InstanceID:     "i-0053c2917e2afa2f0",
					MyBidID:        "sir-yb3gavgp",
					MyMaxPriceUSD:  6.669,
					MarketPriceUSD: 2.001,
					ChargeUSD:      0.073,
					Version:        2,
				},
				{
					AccountId:      testAccountId,
					Timestamp:      time.Date(2021, 02, 20, 18, 50, 58, 0, time.UTC),
					UsageType:      "USW2-SpotUsage:x1.16xlarge",
					Instance:       "x1.16xlarge",
					Operation:      "RunInstances:SV002",
					InstanceID:     "i-07eaa4b2bf27c4b75",
					MyBidID:        "sir-w13gadim",
					MyMaxPriceUSD:  6.669,
					MarketPriceUSD: 2.001,
					ChargeUSD:      1.741,
					Version:        2,
				},
				{
					AccountId:      testAccountId,
					Timestamp:      time.Date(2021, 02, 20, 18, 56, 14, 0, time.UTC),
					UsageType:      "USW2-SpotUsage:x1.32xlarge",
					Instance:       "x1.32xlarge",
					Operation:      "RunInstances:SV002",
					InstanceID:     "i-000e2cebfe213246e",
					MyBidID:        "sir-fcg8btin",
					MyMaxPriceUSD:  13.338,
					MarketPriceUSD: 4.001,
					ChargeUSD:      2.636,
					Version:        2,
				},
				{
					AccountId:      testAccountId,
					Timestamp:      time.Date(2021, 02, 20, 18, 56, 01, 0, time.UTC),
					UsageType:      "USW2-SpotUsage:x1.32xlarge",
					Instance:       "x1.32xlarge",
					Operation:      "RunInstances:SV002",
					InstanceID:     "i-032a1a622fb441a7b",
					MyBidID:        "sir-c6ag9vxn",
					MyMaxPriceUSD:  13.338,
					MarketPriceUSD: 4.001,
					ChargeUSD:      4.001,
					Version:        2,
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			entries, err := test.loader.Fetch(ctx, false)
			require.NoError(t, err, "unexpected error fetching local feed files")
			require.Equal(t, test.expectedEntries, entries)
		})
	}
}

func TestLoaderStream(t *testing.T) {
	ctx := context.Background()
	devNull := log.New(ioutil.Discard, "", 0)

	loader := NewS3Loader("test-bucket", "", &mockS3Client{
		listObjectPageResults: [][]string{
			{
				// initial empty response
			},
			{ // single file response
				testAccountId + ".2100-02-20-15.002.3eb820a5.gz",
			},
		},
		getObjectResults: map[string]string{
			"test-bucket/" + testAccountId + "2100-02-20-15.002.3eb820a5.gz": `#Version: 1.0
#Fields: Timestamp UsageType Operation InstanceID MyBidID MyMaxPrice MarketPrice Charge Version
2100-02-20 18:52:50 UTC	USW2-SpotUsage:x1.16xlarge	RunInstances:SV002	i-0053c2917e2afa2f0	sir-yb3gavgp	6.669 USD	2.001 USD	0.073 USD	2
2100-02-20 18:50:58 UTC	USW2-SpotUsage:x1.16xlarge	RunInstances:SV002	i-07eaa4b2bf27c4b75	sir-w13gadim	6.669 USD	2.001 USD	1.741 USD	2`,
		},
	}, devNull, testAccountId, nil, nil, 0)

	// speed up sleep duration to drain list objects slice
	streamSleepDuration = time.Second

	entryChan, err := loader.Stream(ctx, false)
	require.NoError(t, err, "unexpected err streaming s3 entries")

	// test successful drain of two entries channel
	for i := 0; i < 2; i++ {
		<-entryChan
	}

	// kill the Stream goroutine
	ctx.Done()
}
