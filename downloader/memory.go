package downloader

import (
	"context"
	"sync"
	"time"
)

// Caches downloaded files in memory
type MemoryDownloader struct {
	mutex sync.Mutex
	cache map[string]downloaderCacheEntry

	TimeNow func() time.Time
}

func NewMemoryDownloader() *MemoryDownloader {
	return &MemoryDownloader{
		cache:   make(map[string]downloaderCacheEntry),
		TimeNow: time.Now,
	}
}

type downloaderCacheEntry struct {
	data       []byte
	expiration time.Time
}

func (d *MemoryDownloader) Get(
	ctx context.Context,
	url string,
	headers map[string]string,
	options GetOptions,
) ([]byte, error) {
	if options.Cache {
		d.mutex.Lock()
		defer d.mutex.Unlock()

		if entry, ok := d.cache[url]; ok {
			if entry.expiration.After(d.TimeNow()) {
				return entry.data, nil
			}
		}
	}

	body, err := HTTPGet(ctx, url, headers, options)
	if err != nil {
		return nil, err
	}

	if options.Cache {
		d.cache[url] = downloaderCacheEntry{
			data:       body,
			expiration: d.TimeNow().Add(options.CacheTTL),
		}
	}

	return body, nil
}
