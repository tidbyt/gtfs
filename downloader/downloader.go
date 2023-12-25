package downloader

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

type GetOptions struct {
	MaxSize  int
	Timeout  time.Duration
	Cache    bool
	CacheTTL time.Duration
}

// A thing capable of downloading a file, optionally with caching
type Downloader interface {
	Get(ctx context.Context, url string, headers map[string]string, options GetOptions) ([]byte, error)
}

// Gets a file. Doesn't cache. Provided as convenience for
// implementing custom Downloaders.
func HTTPGet(ctx context.Context, url string, headers map[string]string, options GetOptions) ([]byte, error) {
	client := &http.Client{
		Timeout: options.Timeout,
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	for k, v := range headers {
		req.Header.Add(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("making request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	defer resp.Body.Close()

	var reader io.Reader = resp.Body
	if options.MaxSize > 0 {
		reader = io.LimitReader(resp.Body, int64(options.MaxSize))
	}

	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("reading body: %w", err)
	}

	return body, nil
}
