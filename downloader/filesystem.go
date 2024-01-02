package downloader

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

type Filesystem struct {
	Path    string
	Records map[string]fsRecord

	mutex sync.Mutex
}

type fsRecord struct {
	Body        string `json:"body"`
	RetrievedAt string `json:"retrieved_at"`
}

func NewFilesystem(path string) (*Filesystem, error) {
	fs := &Filesystem{
		Path:    path,
		Records: map[string]fsRecord{},
	}

	err := fs.load()
	if err != nil {
		return nil, err
	}

	return fs, nil
}

func (f *Filesystem) Get(
	ctx context.Context,
	url string,
	headers map[string]string,
	options GetOptions,
) ([]byte, error) {

	f.mutex.Lock()
	defer f.mutex.Unlock()

	if options.Cache {
		if record, found := f.Records[url]; found {
			retrievedAt, err := time.Parse(time.RFC3339, record.RetrievedAt)
			if err != nil {
				return nil, err
			}
			if retrievedAt.Add(options.CacheTTL).After(time.Now()) {
				body, err := base64.StdEncoding.DecodeString(record.Body)
				if err != nil {
					return nil, fmt.Errorf("decoding: %w", err)
				}
				fmt.Println("cache hit")
				return body, nil
			}
			fmt.Println("cache expired")
		}
	}

	body, err := HTTPGet(ctx, url, headers, options)
	if err != nil {
		return nil, fmt.Errorf("http get: %w", err)
	}

	if options.Cache {
		bodyB64 := base64.StdEncoding.EncodeToString(body)
		f.Records[url] = fsRecord{
			Body:        bodyB64,
			RetrievedAt: time.Now().UTC().Format(time.RFC3339),
		}
		err = f.save()
		if err != nil {
			return nil, fmt.Errorf("saving: %w", err)
		}
	}

	return body, nil
}

func (f *Filesystem) load() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	_, err := os.Stat(f.Path)
	if os.IsNotExist(err) {
		return nil
	}

	buf, err := os.ReadFile(f.Path)
	if err != nil {
		return fmt.Errorf("reading: %w", err)
	}

	err = json.Unmarshal(buf, &f.Records)
	if err != nil {
		return fmt.Errorf("unmarshalling: %w", err)
	}

	return nil
}

func (f *Filesystem) save() error {
	buf, err := json.Marshal(f.Records)
	if err != nil {
		return fmt.Errorf("marshalling: %w", err)
	}

	err = os.WriteFile(f.Path, buf, 0644)
	if err != nil {
		return fmt.Errorf("writing: %w", err)
	}

	return nil
}
