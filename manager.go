package gtfs

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"sort"
	"strings"
	"time"

	"tidbyt.dev/gtfs/downloader"
	"tidbyt.dev/gtfs/parse"
	"tidbyt.dev/gtfs/storage"
)

const (
	DefaultStaticRefreshInterval = 12 * time.Hour
	DefaultRealtimeTTL           = 1 * time.Minute
	DefaultRealtimeTimeout       = 30 * time.Second
	DefaultRealtimeMaxSize       = 1 << 20 // 1 MB
	DefaultStaticTimeout         = 60 * time.Second
	DefaultStaticMaxSize         = 800 << 20 // 800 MB
)

var ErrNoActiveFeed = errors.New("no active feed found")

// Manager manages GTFS data.
type Manager struct {
	RealtimeTTL           time.Duration
	RealtimeTimeout       time.Duration
	RealtimeMaxSize       int
	StaticTimeout         time.Duration
	StaticMaxSize         int
	StaticRefreshInterval time.Duration
	Downloader            downloader.Downloader

	storage storage.Storage
}

// Creates a new Manager of GTFS data, on top of the given storage.
//
// By default, the manager will use a an in memory cache for realtime
// data, but not for static schedules as these will be persisted in
// storage.
func NewManager(s storage.Storage) *Manager {
	return &Manager{
		RealtimeTTL:           DefaultRealtimeTTL,
		RealtimeTimeout:       DefaultRealtimeTimeout,
		RealtimeMaxSize:       DefaultRealtimeMaxSize,
		StaticTimeout:         DefaultStaticTimeout,
		StaticMaxSize:         DefaultStaticMaxSize,
		StaticRefreshInterval: DefaultStaticRefreshInterval,

		Downloader: downloader.NewMemory(),

		storage: s,
	}
}

// Loads static GTFS data from a URL
//
// If a feed is available in storage, and active at the given time, it
// is returned immediately. Otherwise, ErrNoActiveFeed is returned.
//
// Unless already present, a FeedRequest for this URL will be placed
// in storage, to track consumers and headers.
func (m *Manager) LoadStaticAsync(
	consumer string,
	staticURL string,
	staticHeaders map[string]string,
	when time.Time,
) (*Static, error) {

	now := time.Now().UTC()

	// Make sure a request exists for this consumer, URL and headers.
	err := m.storage.WriteFeedRequest(storage.FeedRequest{
		URL: staticURL,
		Consumers: []storage.FeedConsumer{
			{
				Name:      consumer,
				Headers:   serializeHeaders(staticHeaders),
				CreatedAt: now,
				UpdatedAt: now,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("writing feed request: %w", err)
	}

	// Attempt to load the feed from storage.
	feeds, err := m.storage.ListFeeds(storage.ListFeedsFilter{URL: staticURL})
	if err != nil {
		return nil, fmt.Errorf("listing feeds: %w", err)
	}

	return m.loadMostRecentActive(feeds, when)
}

// Loads realtime GTFS data from a static and realtime feed.
func (m *Manager) LoadRealtime(
	consumer string,
	staticURL string,
	staticHeaders map[string]string,
	realtimeURL string,
	realtimeHeaders map[string]string,
	when time.Time,
) (*Realtime, error) {

	static, err := m.LoadStaticAsync(consumer, staticURL, staticHeaders, when)
	if err != nil {
		return nil, fmt.Errorf("loading static: %w", err)
	}

	feedData, err := m.Downloader.Get(
		context.Background(),
		realtimeURL,
		realtimeHeaders,
		downloader.GetOptions{
			Cache:    true,
			CacheTTL: m.RealtimeTTL,
			Timeout:  m.RealtimeTimeout,
			MaxSize:  m.RealtimeMaxSize,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("downloading realtime: %w", err)
	}

	realtime, err := NewRealtime(context.Background(), static, [][]byte{feedData})
	if err != nil {
		return nil, fmt.Errorf("creating realtime: %w", err)
	}

	return realtime, nil
}

// Refreshes any feeds that might need refreshing.
func (m *Manager) Refresh(ctx context.Context) error {

	// Get the hash of every feed in storage
	feedsByHash := map[string][]*storage.FeedMetadata{}
	feeds, err := m.storage.ListFeeds(storage.ListFeedsFilter{})
	if err != nil {
		return fmt.Errorf("listing feeds: %w", err)
	}
	for _, feed := range feeds {
		feedsByHash[feed.Hash] = append(feedsByHash[feed.Hash], feed)
	}

	// Check all requests for URLs in need of refreshing
	requests, err := m.storage.ListFeedRequests("")
	if err != nil {
		return fmt.Errorf("listing feed requests: %w", err)
	}

	errs := []error{}
	for _, req := range requests {
		if req.RefreshedAt.Before(time.Now().Add(-m.StaticRefreshInterval)) {
			err = m.processRequest(req, feedsByHash)
			if err != nil {
				errs = append(errs, fmt.Errorf("refreshing feed at %s: %w", req.URL, err))
			}
		}
	}

	return errors.Join(errs...)
}

// Downloads a requested URL. A randomly selected consumer's headers
// will be used. If the data is already in storage, a copy may be made
// to ensure a FeedMetadata record with the hash and this URL
// exists. New FeedMetadata records are added to the feedByHash map
// passed in as arg.
func (m *Manager) processRequest(
	req storage.FeedRequest,
	feedByHash map[string][]*storage.FeedMetadata,
) error {

	// Get headers from a random consumer
	headers, err := deserializeHeaders(req.Consumers[rand.Intn(len(req.Consumers))].Headers)
	if err != nil {
		return fmt.Errorf("deserializing headers: %w", err)
	}

	// Download the feed and compute its hash
	body, err := m.Downloader.Get(
		context.Background(),
		req.URL,
		headers,
		downloader.GetOptions{
			Cache:   false,
			Timeout: m.StaticTimeout,
			MaxSize: m.StaticMaxSize,
		},
	)
	if err != nil {
		return fmt.Errorf("downloading feed at %s: %w", req.URL, err)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(body))

	// The data we just downloaded may already exist in storage.
	feeds := feedByHash[hash]
	if len(feeds) > 0 {
		found := false
		for _, feed := range feeds {
			if feed.URL == req.URL {
				found = true
				break
			}
		}
		if !found {
			// It's in storage, but for a different
			// URL. Add a metadata record for this URL.
			metadata := feeds[0]
			metadata.URL = req.URL

			feedByHash[hash] = append(feedByHash[hash], metadata)

			err = m.storage.WriteFeedMetadata(metadata)
			if err != nil {
				return fmt.Errorf("writing metadata: %w", err)
			}
		} else {
			// Hash exists for this same URL. Nothing to do.
		}
	} else {
		// Hash doesn't exist in storage. Parse the feed.
		writer, err := m.storage.GetWriter(hash)
		if err != nil {
			return fmt.Errorf("getting writer: %w", err)
		}
		defer writer.Close()

		metadata, err := parse.ParseStatic(writer, body)
		if err != nil {

			// If the downloaded data is broken (parse
			// failed), we still mark the request as
			// refreshed.
			req.RefreshedAt = time.Now().UTC()
			reqErr := m.storage.WriteFeedRequest(req)
			if reqErr != nil {
				return errors.Join(
					fmt.Errorf("writing feed request: %w", reqErr),
					fmt.Errorf("parsing: %w", err),
				)
			}

			return fmt.Errorf("parsing: %w", err)
		}

		// And write the metadata
		metadata.Hash = hash
		metadata.URL = req.URL
		metadata.RetrievedAt = time.Now().UTC()

		feedByHash[hash] = append(feedByHash[hash], metadata)

		err = m.storage.WriteFeedMetadata(metadata)
		if err != nil {
			return fmt.Errorf("writing metadata: %w", err)
		}
	}

	// Mark the request as refreshed.
	req.RefreshedAt = time.Now().UTC()
	err = m.storage.WriteFeedRequest(req)
	if err != nil {
		return fmt.Errorf("writing feed request: %w", err)
	}

	return nil
}

// Selects the most recently retrieved feed from feeds that is also
// active at the given time.
func (m *Manager) loadMostRecentActive(feeds []*storage.FeedMetadata, when time.Time) (*Static, error) {
	sort.Slice(feeds, func(i, j int) bool {
		return feeds[i].RetrievedAt.Before(feeds[j].RetrievedAt)
	})

	for i := len(feeds) - 1; i >= 0; i-- {
		ok, err := feedActive(feeds[i], when)
		if err != nil {
			return nil, fmt.Errorf("checking if feed is active: %w", err)
		}
		if !ok {
			continue
		}

		// This is the one!
		reader, err := m.storage.GetReader(feeds[i].Hash)
		if err != nil {
			return nil, fmt.Errorf("getting reader: %w", err)
		}
		static, err := NewStatic(reader, feeds[i])
		if err != nil {
			return nil, fmt.Errorf("creating static: %w", err)
		}
		return static, nil
	}

	// No active feed found.
	return nil, ErrNoActiveFeed
}

func feedActive(feed *storage.FeedMetadata, now time.Time) (bool, error) {
	feedTz, err := time.LoadLocation(feed.Timezone)
	if err != nil {
		return false, fmt.Errorf("loading timezone: %w", err)
	}

	nowThere := now.In(feedTz)
	todayThere := time.Date(
		nowThere.Year(),
		nowThere.Month(),
		nowThere.Day(),
		0, 0, 0, 0,
		feedTz,
	).Format("20060102")

	if feed.CalendarStartDate > todayThere {
		return false, nil
	}
	if feed.CalendarEndDate < todayThere {
		return false, nil
	}

	return true, nil
}

func serializeHeaders(headers map[string]string) string {
	var keys []string
	for k := range headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var serialized string
	for _, k := range keys {
		serialized += fmt.Sprintf("%s=%s", url.QueryEscape(k), url.QueryEscape(headers[k]))
	}
	return serialized
}

func deserializeHeaders(serialized string) (map[string]string, error) {
	headers := map[string]string{}
	if serialized == "" {
		return headers, nil
	}

	var err error
	for _, pair := range strings.Split(serialized, "&") {
		parts := strings.Split(pair, "=")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid header: %s", pair)
		}
		headers[parts[0]], err = url.QueryUnescape(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid header: %s", pair)
		}
	}
	return headers, nil
}
