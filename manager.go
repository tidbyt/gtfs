package gtfs

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
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
	RefreshInterval time.Duration
	RealtimeTTL     time.Duration
	RealtimeTimeout time.Duration
	RealtimeMaxSize int
	StaticTimeout   time.Duration
	StaticMaxSize   int
	Downloader      downloader.Downloader
	storage         storage.Storage
}

// Creates a new Manager of GTFS data, on top of the given storage.
//
// By default, the manager will use a an in memory cache for realtime
// data, but not for static schedules as these will be persisted in
// storage.
func NewManager(s storage.Storage) *Manager {
	return &Manager{
		RefreshInterval: DefaultStaticRefreshInterval,
		RealtimeTTL:     DefaultRealtimeTTL,
		RealtimeTimeout: DefaultRealtimeTimeout,
		RealtimeMaxSize: DefaultRealtimeMaxSize,
		StaticTimeout:   DefaultStaticTimeout,
		StaticMaxSize:   DefaultStaticMaxSize,

		Downloader: downloader.NewMemoryDownloader(),

		storage: s,
	}
}

// Loads static GTFS data from a URL
//
// If a feed is available in storage, and active at the given time, it
// is returned immediately. Otherwise, ErrNoActiveFeed is returned.
//
// If the URL is previously unseen, a marker is left in storage for a
// later call to RefreshFeeds() to retrieve it.
func (m *Manager) LoadStaticAsync(url string, when time.Time) (*Static, error) {

	feeds, err := m.storage.ListFeeds(storage.ListFeedsFilter{URL: url})
	if err != nil {
		return nil, fmt.Errorf("listing feeds: %w", err)
	}

	if len(feeds) == 0 {
		fmt.Println("No feeds found. Adding record to request it.")
		// No feeds found. Add record to request it.
		err = m.storage.WriteFeedMetadata(&storage.FeedMetadata{
			URL: url,
		})
		if err != nil {
			return nil, fmt.Errorf("writing feed metadata: %w", err)
		}
		return nil, ErrNoActiveFeed
	}

	return m.loadMostRecentActive(feeds, when)
}

// Loads static GTFS data from a URL
//
// If the URL is previously unseen, it is retrieved immediately.
//
// If no feed is active at the given time, ErrNoActiveFeed is
// returned.
func (m *Manager) LoadStatic(url string, when time.Time) (*Static, error) {

	// All feeds for URL, sorted by retrieval time
	feeds, err := m.storage.ListFeeds(storage.ListFeedsFilter{URL: url})
	if err != nil {
		return nil, fmt.Errorf("listing feeds: %w", err)
	}

	sort.Slice(feeds, func(i, j int) bool {
		return feeds[i].RetrievedAt.Before(feeds[j].RetrievedAt)
	})

	// If we don't have it, get it
	if len(feeds) == 0 {
		metadata, err := m.refreshStatic(url)
		if err != nil {
			return nil, fmt.Errorf("refreshing static: %w", err)
		}

		err = m.storage.WriteFeedMetadata(metadata)
		if err != nil {
			return nil, fmt.Errorf("writing metadata: %w", err)
		}

		feeds = []*storage.FeedMetadata{metadata}
	}

	return m.loadMostRecentActive(feeds, when)
}

// Loads realtime GTFS data from a static and realtime feed.
func (m *Manager) LoadRealtime(
	staticURL string,
	staticHeaders map[string]string,
	realtimeURL string,
	realtimeHeaders map[string]string,
	when time.Time,
) (*Realtime, error) {

	static, err := m.LoadStatic(staticURL, when)
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
			TimeOut:  m.RealtimeTimeout,
			MaxSize:  m.RealtimeMaxSize,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("downloading realtime: %w", err)
	}

	fmt.Println("got", len(feedData), "bytes of realtime data")

	realtime, err := NewRealtime(context.Background(), static, [][]byte{feedData})
	if err != nil {
		return nil, fmt.Errorf("creating realtime: %w", err)
	}

	return realtime, nil
}

// Refreshes any feeds that might need refreshing.
func (m *Manager) Refresh(ctx context.Context) error {

	// Get all feeds, group by URL
	feeds, err := m.storage.ListFeeds(storage.ListFeedsFilter{})
	if err != nil {
		return fmt.Errorf("listing feeds: %w", err)
	}
	feedsByURL := make(map[string][]*storage.FeedMetadata)
	for _, feed := range feeds {
		feedsByURL[feed.URL] = append(feedsByURL[feed.URL], feed)
	}

	// Process each URL
	for url, feeds := range feedsByURL {
		err = m.refreshFeeds(url, feeds)
		if err != nil {
			return fmt.Errorf("refreshing %s: %w", url, err)
		}
	}

	return nil
}

func (m *Manager) refreshFeeds(url string, feeds []*storage.FeedMetadata) error {
	// If there's only one record, and it has no SHA256, it's an
	// async request for feed retrieval.
	if len(feeds) == 1 && feeds[0].SHA256 == "" {
		fmt.Printf("Refreshing async %s\n", url)
		metadata, err := m.refreshStatic(url)
		if err != nil {
			return fmt.Errorf("refreshing static at %s: %w", url, err)
		}

		err = m.storage.WriteFeedMetadata(metadata)
		if err != nil {
			return fmt.Errorf("writing metadata: %w", err)
		}

		// delete the async request
		err = m.storage.DeleteFeedMetadata(url, "")
		if err != nil {
			return fmt.Errorf("deleting metadata: %w", err)
		}

		return nil
	}

	fmt.Printf("Refreshing existing %s\n", url)

	// If the most recently retrieved feed is outdated, it's
	// refresh time
	sort.Slice(feeds, func(i, j int) bool {
		return feeds[j].RetrievedAt.Before(feeds[i].RetrievedAt)
	})
	if !feeds[0].RetrievedAt.IsZero() && feeds[0].RetrievedAt.Add(m.RefreshInterval).Before(time.Now()) {
		metadata, err := m.refreshStatic(url)
		if err != nil {
			return fmt.Errorf("refreshing static at %s: %w", url, err)
		}

		err = m.storage.WriteFeedMetadata(metadata)
		if err != nil {
			return fmt.Errorf("writing metadata: %w", err)
		}
	}

	return nil
}

// Refreshes a static feed from a URL. Returns the feed metadata if
// successful. Note that the feed may already be in storage from a
// previous refresh.
func (m *Manager) refreshStatic(url string) (*storage.FeedMetadata, error) {

	// TODO: add support for ETag?

	// GET the feed
	body, err := m.Downloader.Get(
		context.Background(),
		url,
		nil,
		downloader.GetOptions{
			Cache:   false,
			Timeout: m.StaticTimeout,
			MaxSize: m.StaticMaxSize,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("downloading: %w", err)
	}

	// Check if this exact feed is already in storage
	hash := fmt.Sprintf("%x", sha256.Sum256(body))
	feeds, err := m.storage.ListFeeds(storage.ListFeedsFilter{SHA256: hash})
	if err != nil {
		return nil, fmt.Errorf("listing feeds: %w", err)
	}
	if len(feeds) > 0 {
		for _, feed := range feeds {
			if feed.URL != url {
				// Found, but from a different
				// URL. Add a record for this URL so
				// future lookups can find it.
				feed.URL = url
				feed.UpdatedAt = time.Now()
				err = m.storage.WriteFeedMetadata(feed)
				if err != nil {
					return nil, fmt.Errorf("writing metadata: %w", err)
				}

				return feed, nil
			}
		}

		// Found, and from the same URL. Update timestamp
		// indicating last refresh, and return.
		feeds[0].UpdatedAt = time.Now()
		err = m.storage.WriteFeedMetadata(feeds[0])
		if err != nil {
			return nil, fmt.Errorf("writing metadata: %w", err)
		}

		return feeds[0], nil
	}

	// Feed is brand new to us. Parse and write to storage.
	writer, err := m.storage.GetWriter(hash)
	if err != nil {
		return nil, fmt.Errorf("getting writer: %w", err)
	}
	defer writer.Close()

	metadata, err := parse.ParseStatic(writer, body)
	if err != nil {
		// Parse failure is special. If something fails to
		// parse now, there's no reason to retry
		// soon. Instead, we treat parse failure as if the
		// data simply hasn't been updated.
		feeds, listErr := m.storage.ListFeeds(storage.ListFeedsFilter{URL: url})
		if listErr != nil {
			return nil, fmt.Errorf("listing feeds: %w", listErr)
		}
		if len(feeds) > 0 {
			sort.Slice(feeds, func(i, j int) bool {
				return feeds[i].RetrievedAt.After(feeds[j].RetrievedAt)
			})
			feeds[0].UpdatedAt = time.Now()
			writeErr := m.storage.WriteFeedMetadata(feeds[0])
			if writeErr != nil {
				return nil, fmt.Errorf("writing metadata: %w", writeErr)
			}
		}

		return nil, fmt.Errorf("parsing feed: %w", err)
	}

	metadata.SHA256 = hash
	metadata.URL = url
	metadata.RetrievedAt = time.Now()
	metadata.UpdatedAt = metadata.RetrievedAt

	return metadata, nil
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

	if feed.FeedStartDate != "" && feed.FeedStartDate > todayThere {
		return false, nil
	}
	if feed.FeedEndDate != "" && feed.FeedEndDate < todayThere {
		return false, nil
	}
	if feed.CalendarStartDate > todayThere {
		return false, nil
	}
	if feed.CalendarEndDate < todayThere {
		return false, nil
	}

	return true, nil
}

// Selects he most recently retrieved feed from feeds that is also
// active at the given time.
func (m *Manager) loadMostRecentActive(feeds []*storage.FeedMetadata, when time.Time) (*Static, error) {
	sort.Slice(feeds, func(i, j int) bool {
		return feeds[i].RetrievedAt.Before(feeds[j].RetrievedAt)
	})

	for i := len(feeds) - 1; i >= 0; i-- {
		fmt.Printf("Considering feed %s %s\n", feeds[i].URL, feeds[i].SHA256)

		ok, err := feedActive(feeds[i], when)
		if err != nil {
			return nil, fmt.Errorf("checking if feed is active: %w", err)
		}
		if !ok {
			fmt.Printf("Feed %s is not active\n", feeds[i].URL)
			continue
		}

		// This is the one!
		reader, err := m.storage.GetReader(feeds[i].SHA256)
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

func (m *Manager) buildStatic(feed *storage.FeedMetadata) (*Static, error) {
	reader, err := m.storage.GetReader(feed.SHA256)
	if err != nil {
		return nil, fmt.Errorf("getting reader: %w", err)
	}
	static, err := NewStatic(reader, feed)
	if err != nil {
		return nil, fmt.Errorf("creating static: %w", err)
	}
	return static, nil
}
