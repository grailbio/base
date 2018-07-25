// Package spotfeed is used for querying spot-data-feeds provided by AWS.
// See https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-data-feeds.html for a description of the
// spot data feed format.
//
// This package provides two interfaces for interacting with the AWS spot data feed format for files hosted
// on S3.
//
// 1. Fetch  - makes a single blocking call to fetch feed files for some historical period, then parses and
//             returns the results as a single slice.
// 2. Stream - creates a goroutine that asynchronously checks (once per 30mins by default) the specified S3
//             location for new spot data feed files (and sends parsed entries into a channel provided to
//             the user at invocation).
//
// This package also provides a LocalLoader which can perform a Fetch operation against feed files already
// downloaded to local disk. This is often useful for analyzing spot usage over long periods of time, since
// the download phase can take some time.
package spotfeed

import (
	"compress/gzip"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/retry"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

var (
	// RetryPolicy is used to retry failed S3 API calls.
	retryPolicy = retry.Backoff(time.Second, 10*time.Second, 2)

	// Used to rate limit S3 calls.
	limiter = rate.NewLimiter(rate.Limit(16), 4)
)

type filterable interface {
	accountId() string
	timestamp() time.Time
	version() int64
}

type filters struct {
	// AccountId configures the Loader to only return Entry objects that belong to the specified
	// 12-digit AWS account number (ID). If zero, no AccountId filter is applied.
	AccountId string

	// StartTime configures the Loader to only return Entry objects younger than StartTime.
	// If nil, no StartTime filter is applied.
	StartTime *time.Time

	// EndTime configures the Loader to only return Entry objects older than EndTime.
	// If nil, no EndTime filter is applied.
	EndTime *time.Time

	// Version configures the Loader to only return Entry objects with version equal to Version.
	// If zero, no Version filter is applied, and if multiple feed versions declare the same
	// instance-hour, de-duping based on the maximum value seen for that hour will be applied.
	Version int64
}

// filter returns true if the entry does not match loader criteria and should be filtered out.
func (l *filters) filter(f filterable) bool {
	if l.AccountId != "" && f.accountId() != l.AccountId {
		return true
	}
	if l.StartTime != nil && f.timestamp().Before(*l.StartTime) { // inclusive
		return true
	}
	if l.EndTime != nil && !f.timestamp().Before(*l.EndTime) { // exclusive
		return true
	}
	if l.Version != 0 && f.version() != l.Version {
		return true
	}
	return false
}

// filterTruncatedStartTime performs the same checks as filter but truncates the start boundary down to the hour.
func (l *filters) filterTruncatedStartTime(f filterable) bool {
	if l.AccountId != "" && f.accountId() != l.AccountId {
		return true
	}
	if l.StartTime != nil {
		truncatedStart := l.StartTime.Truncate(time.Hour)
		if f.timestamp().Before(truncatedStart) { // inclusive
			return true
		}
	}
	if l.EndTime != nil && !f.timestamp().Before(*l.EndTime) { // exclusive
		return true
	}
	if l.Version != 0 && f.version() != l.Version {
		return true
	}
	return false

}

type localFile struct {
	*fileMeta
	path string
}

func (f *localFile) read() ([]*Entry, error) {
	fd, err := os.Open(f.path)
	defer func() { _ = fd.Close() }()
	if err != nil {
		err = errors.E(err, fmt.Sprintf("failed to open local spot feed data file %s", f.path))
		return nil, err
	}

	if f.IsGzip {
		gz, err := gzip.NewReader(fd)
		defer func() { _ = gz.Close() }()
		if err != nil {
			return nil, fmt.Errorf("failed to read gzipped file %s", f.Name)
		}
		return ParseFeedFile(gz, f.AccountId)
	}

	return ParseFeedFile(fd, f.AccountId)
}

type s3File struct {
	*fileMeta
	bucket, key string
	client      s3iface.S3API
}

func (s *s3File) read(ctx context.Context) ([]*Entry, error) {
	// Pull feed file from S3 with rate limiting and retries.
	var output *s3.GetObjectOutput
	for retries := 0; ; {
		if err := limiter.Wait(ctx); err != nil {
			return nil, err
		}
		var getObjErr error
		if output, getObjErr = s.client.GetObjectWithContext(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(s.key),
		}); getObjErr != nil {
			if !request.IsErrorThrottle(getObjErr) {
				return nil, getObjErr
			}
			if err := retry.Wait(ctx, retryPolicy, retries); err != nil {
				return nil, err
			}
			retries++
			continue
		}
		break
	}
	// If the file is gzipped, unpack before attempting to read.
	if s.IsGzip {
		gz, err := gzip.NewReader(output.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read gzipped file s3://%s/%s", s.bucket, s.key)
		}
		defer func() { _ = gz.Close() }()
		return ParseFeedFile(gz, s.AccountId)
	}

	return ParseFeedFile(output.Body, s.AccountId)
}

// Loader provides an API for pulling Spot Data Feed Entry objects from some repository.
// The tolerateErr parameter configures how the Loader responds to errors parsing
// individual files or entries; if true, the Loader will continue to parse and yield Entry
// objects if an error is encountered during parsing.
type Loader interface {
	// Fetch performs a single blocking call to fetch a discrete set of Entry objects.
	Fetch(ctx context.Context, tolerateErr bool) ([]*Entry, error)

	// Stream asynchronously retrieves, parses and sends Entry objects on the returned channel.
	// To graciously terminate the goroutine managing the Stream, the client terminates the given context.
	Stream(ctx context.Context, tolerateErr bool) (<-chan *Entry, error)
}

type s3Loader struct {
	Loader
	filters

	log     *log.Logger
	client  s3iface.S3API
	bucket  string
	rootURI string
}

// commonFilePrefix returns the most specific prefix common to all spot feed data files that
// match the loader criteria.
func (s *s3Loader) commonFilePrefix() string {
	if s.AccountId == "" {
		return ""
	}

	if s.StartTime == nil || s.EndTime == nil || s.StartTime.Year() != s.EndTime.Year() {
		return s.AccountId
	}

	if s.StartTime.Month() != s.EndTime.Month() {
		return fmt.Sprintf("%s.%d", s.AccountId, s.StartTime.Year())
	}

	if s.StartTime.Day() != s.EndTime.Day() {
		return fmt.Sprintf("%s.%d-%02d", s.AccountId, s.StartTime.Year(), s.StartTime.Month())
	}

	if s.StartTime.Hour() != s.EndTime.Hour() {
		return fmt.Sprintf("%s.%d-%02d-%02d", s.AccountId, s.StartTime.Year(), s.StartTime.Month(), s.StartTime.Day())
	}

	return fmt.Sprintf("%s.%d-%02d-%02d-%02d", s.AccountId, s.StartTime.Year(), s.StartTime.Month(), s.StartTime.Day(), s.StartTime.Hour())
}

// timePrefix returns a prefix which matches the given time in UTC.
func (s *s3Loader) timePrefix(t time.Time) string {
	if s.AccountId == "" {
		panic("nowPrefix cannot be given without an account id")
	}

	t = t.UTC()
	return fmt.Sprintf("%s.%d-%02d-%02d-%02d", s.AccountId, t.Year(), t.Month(), t.Day(), t.Hour())
}

// path returns a prefix which joins the loader rootURI with the given uri.
func (s *s3Loader) path(uri string) string {
	if s.rootURI == "" {
		return uri
	} else {
		return fmt.Sprintf("%s/%s", s.rootURI, uri)
	}
}

// list queries the AWS S3 ListBucket API for feed files.
func (s *s3Loader) list(ctx context.Context, startAfter string, tolerateErr bool) ([]*s3File, error) {
	prefix := s.path(s.commonFilePrefix())

	s3Files := make([]*s3File, 0)
	var parseMetaErr error
	if err := s.client.ListObjectsV2PagesWithContext(ctx, &s3.ListObjectsV2Input{
		Bucket:     aws.String(s.bucket),
		Prefix:     aws.String(prefix),
		StartAfter: aws.String(startAfter),
	}, func(output *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, object := range output.Contents {
			filename := aws.StringValue(object.Key)
			fileMeta, err := parseFeedFileName(filename)
			if err != nil {
				parseMetaErr = errors.E(err, fmt.Sprintf("failed to parse spot feed data file name %s", filename))
				if tolerateErr {
					s.log.Print(parseMetaErr)
					continue
				} else {
					return false
				}
			}

			// skips s3Files that do not match the loader criteria. Truncate the startTime of the filter to ensure that
			// we do not skip files at hour HH:00 with a startTime of (i.e.) HH:30.
			if s.filterTruncatedStartTime(fileMeta) {
				s.log.Printf("%s does not pass fileMeta filter, skipping", filename)
				continue
			}
			s3Files = append(s3Files, &s3File{
				fileMeta,
				s.bucket,
				filename,
				s.client,
			})
		}
		return true
	}); err != nil {
		return nil, fmt.Errorf("list on path %s failed with error: %s", prefix, err)
	}
	if !tolerateErr && parseMetaErr != nil {
		return nil, parseMetaErr
	}
	return s3Files, nil
}

// fetchAfter builds a list of S3 feed file objects using the S3 ListBucket API. It then concurrently
// fetches and parses the feed files, observing rate and concurrency limits.
func (s *s3Loader) fetchAfter(ctx context.Context, startAfter string, tolerateErr bool) ([]*Entry, error) {
	s3Files, err := s.list(ctx, startAfter, tolerateErr)
	if err != nil {
		return nil, err
	}

	mu := &sync.Mutex{}
	spotDataEntries := make([]*Entry, 0)
	group, groupCtx := errgroup.WithContext(ctx)
	for _, file := range s3Files {
		file := file
		group.Go(func() error {
			if entries, err := file.read(groupCtx); err != nil {
				err = errors.E(err, fmt.Sprintf("failed to parse spot feed data file s3://%s/%s", file.bucket, file.key))
				if tolerateErr {
					s.log.Printf("encountered error %s, tolerating and skipping file s3://%s/%s", err, file.bucket, file.key)
					return nil
				} else {
					return err
				}
			} else {
				mu.Lock()
				spotDataEntries = append(spotDataEntries, entries...)
				mu.Unlock()
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}

	filteredEntries := make([]*Entry, 0)
	for _, e := range spotDataEntries {
		if !s.filter(e) {
			filteredEntries = append(filteredEntries, e)
		}
	}

	return filteredEntries, nil
}

// Fetch makes a single blocking call to fetch feed files for some historical period,
// then parses and returns the results as a single slice. The call attempts to start
// from the first entry such that Key > l.StartTime and breaks when it encounters the
// first entry such that Key > l.EndTime
func (s *s3Loader) Fetch(ctx context.Context, tolerateErr bool) ([]*Entry, error) {
	prefix := s.path(s.commonFilePrefix())
	return s.fetchAfter(ctx, prefix, tolerateErr)
}

var (
	// streamSleepDuration specifies how long to wait between calls to S3 ListBucket
	streamSleepDuration = 30 * time.Minute
)

// Stream creates a goroutine that asynchronously checks (once per 30mins by default) the specified S3
// location for new spot data feed files (and sends parsed entries into a channel provided to the user at invocation).
// s3Loader must be configured with an account id to support the Stream interface. To stream events for multiple account ids
// which share a feed bucket, create multiple s3Loader objects.
// TODO: Allow caller to pass channel, allowing a single reader to manage multiple s3Loader.Stream calls.
func (s *s3Loader) Stream(ctx context.Context, tolerateErr bool) (<-chan *Entry, error) {
	if s.AccountId == "" {
		return nil, fmt.Errorf("s3Loader must be configured with an account id to provide asynchronous event streaming")
	}

	entryChan := make(chan *Entry)
	go func() {
		startAfter := s.timePrefix(time.Now())
		for {
			if ctx.Err() != nil {
				close(entryChan)
				return
			}

			entries, err := s.fetchAfter(ctx, startAfter, tolerateErr)
			if err != nil {
				close(entryChan)
				return
			}

			for _, entry := range entries {
				entryChan <- entry
			}

			if len(entries) != 0 {
				finalEntry := entries[len(entries)-1]
				startAfter = s.timePrefix(finalEntry.Timestamp)
			}

			time.Sleep(streamSleepDuration)
		}
	}()

	return entryChan, nil
}

// NewSpotFeedLoader returns a Loader which queries the spot data feed subscription using the given session and
// returns a Loader which queries the S3 API for feed files (if a subscription does exist).
// NewSpotFeedLoader will return an error if the spot data feed subscription is missing.
func NewSpotFeedLoader(sess *session.Session, log *log.Logger, startTime, endTime *time.Time, version int64) (Loader, error) {
	ec2api := ec2.New(sess)
	resp, err := ec2api.DescribeSpotDatafeedSubscription(&ec2.DescribeSpotDatafeedSubscriptionInput{})
	if err != nil {
		return nil, errors.E("DescribeSpotDatafeedSubscription", err)
	}
	bucket := aws.StringValue(resp.SpotDatafeedSubscription.Bucket)
	rootURI := aws.StringValue(resp.SpotDatafeedSubscription.Prefix)
	accountID := aws.StringValue(resp.SpotDatafeedSubscription.OwnerId)
	return NewS3Loader(bucket, rootURI, s3.New(sess), log, accountID, startTime, endTime, version), nil
}

// NewS3Loader returns a Loader which queries the S3 API for feed files. It supports the Fetch and Stream APIs.
func NewS3Loader(bucket, rootURI string, client s3iface.S3API, log *log.Logger, accountId string, startTime, endTime *time.Time, version int64) Loader {
	// Remove any trailing slash from bucket and trailing/leading slash from rootURI.
	if strings.HasSuffix(bucket, "/") {
		bucket = bucket[:len(bucket)-1]
	}
	if strings.HasPrefix(rootURI, "/") {
		rootURI = rootURI[1:]
	}
	if strings.HasSuffix(rootURI, "/") {
		rootURI = rootURI[:len(rootURI)-1]
	}

	return &s3Loader{
		filters: filters{
			AccountId: accountId,
			StartTime: startTime,
			EndTime:   endTime,
			Version:   version,
		},
		log:     log,
		client:  client,
		bucket:  bucket,
		rootURI: rootURI,
	}
}

type localLoader struct {
	Loader
	filters

	log      *log.Logger
	rootPath string
}

// Fetch queries the local filesystem for feed files at the given path which match the given filename filters.
// It then parses, filters again and returns the Entry objects.
func (l *localLoader) Fetch(ctx context.Context, tolerateErr bool) ([]*Entry, error) {
	// Iterate over files in directory, filter and build slice of feed files.
	spotFiles := make([]*localFile, 0)
	items, _ := ioutil.ReadDir(l.rootPath)
	for _, item := range items {
		// Skip subdirectories.
		if item.IsDir() {
			continue
		}

		p := path.Join(l.rootPath, item.Name())
		fileMeta, err := parseFeedFileName(item.Name())
		if err != nil {
			err = errors.E(err, fmt.Sprintf("failed to parse spot feed data file name %s", p))
			if tolerateErr {
				l.log.Printf("encountered error %s, tolerating and skipping file %s", err, p)
				continue
			} else {
				return nil, err
			}
		}

		// skips files that do not match the loader criteria. Truncate the startTime of the filter to ensure that
		// we do not skip files at hour HH:00 with a startTime of (i.e.) HH:30.
		if l.filterTruncatedStartTime(fileMeta) {
			l.log.Printf("%s does not pass fileMeta filter, skipping", p)
			continue
		}

		spotFiles = append(spotFiles, &localFile{
			fileMeta,
			p,
		})
	}

	// Concurrently iterate over spot data feed files and build a slice of entries.
	mu := &sync.Mutex{}
	spotDataEntries := make([]*Entry, 0)
	group, _ := errgroup.WithContext(ctx)
	for _, file := range spotFiles {
		file := file
		group.Go(func() error {
			if entries, err := file.read(); err != nil {
				err = errors.E(err, fmt.Sprintf("failed to parse spot feed data file %s", file.path))
				if tolerateErr {
					l.log.Printf("encountered error %s, tolerating and skipping file %s", err, file.path)
					return nil
				} else {
					return err
				}
			} else {
				mu.Lock()
				spotDataEntries = append(spotDataEntries, entries...)
				mu.Unlock()
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}

	// Filter entries
	filteredEntries := make([]*Entry, 0)
	for _, e := range spotDataEntries {
		if !l.filter(e) {
			filteredEntries = append(filteredEntries, e)
		}
	}

	return filteredEntries, nil
}

// NewLocalLoader returns a Loader which fetches feed files from a path on the local filesystem. It does not support
// the Stream API.
func NewLocalLoader(path string, log *log.Logger, accountId string, startTime, endTime *time.Time, version int64) Loader {
	return &localLoader{
		filters: filters{
			AccountId: accountId,
			StartTime: startTime,
			EndTime:   endTime,
			Version:   version,
		},
		log:      log,
		rootPath: path,
	}
}
