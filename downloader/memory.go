package downloader

import (
	"context"
	"sync"
	"time"
)

// Caches downloaded files in memory
type Memory struct {
	mutex   sync.Mutex
	records map[string]memoryRecord

	TimeNow func() time.Time
}

type memoryRecord struct {
	data       []byte
	expiration time.Time
}

func NewMemory() *Memory {
	return &Memory{
		records: map[string]memoryRecord{},
		TimeNow: time.Now,
	}
}

func (d *Memory) Get(
	ctx context.Context,
	url string,
	headers map[string]string,
	options GetOptions,
) ([]byte, error) {
	if options.Cache {
		d.mutex.Lock()
		defer d.mutex.Unlock()

		if record, ok := d.records[url]; ok {
			if record.expiration.After(d.TimeNow()) {
				return record.data, nil
			}
		}
	}

	body, err := HTTPGet(ctx, url, headers, options)
	if err != nil {
		return nil, err
	}

	if options.Cache {
		d.records[url] = memoryRecord{
			data:       body,
			expiration: d.TimeNow().Add(options.CacheTTL),
		}
	}

	return body, nil
}
