package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"tidbyt.dev/gtfs"
	"tidbyt.dev/gtfs/downloader"
	"tidbyt.dev/gtfs/storage"
)

var rootCmd = &cobra.Command{
	Use:          "gtfs",
	Short:        "Tidbyt GTFS tool",
	Long:         "Does stuff with GTFS data",
	SilenceUsage: true,
}

var (
	staticURL       string
	realtimeURL     string
	staticHeaders   []string
	realtimeHeaders []string
	sharedHeaders   []string
)

func init() {
	rootCmd.PersistentFlags().StringVarP(&staticURL, "static-url", "", "", "GTFS Static URL")
	rootCmd.PersistentFlags().StringVarP(&realtimeURL, "realtime-url", "", "", "GTFS Realtime URL")
	rootCmd.PersistentFlags().StringSliceVarP(
		&staticHeaders,
		"static-header",
		"",
		[]string{},
		"GTFS Static HTTP header",
	)
	rootCmd.PersistentFlags().StringSliceVarP(
		&realtimeHeaders,
		"realtime-header",
		"",
		[]string{},
		"GTFS Realtime HTTP header",
	)
	rootCmd.PersistentFlags().StringSliceVarP(
		&sharedHeaders,
		"header",
		"",
		[]string{},
		"GTFS HTTP header (shared between static and realtime)",
	)
	rootCmd.AddCommand(departuresCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func parseHeaders(headers []string) (map[string]string, error) {
	parsed := map[string]string{}
	for _, header := range headers {
		parts := strings.SplitN(header, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("'%s' is not on form <key>:<value>", header)
		}
		parsed[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	}
	return parsed, nil
}

func LoadStaticFeed() (*gtfs.Static, error) {
	if staticURL == "" {
		return nil, fmt.Errorf("static URL is required")
	}

	s, err := storage.NewSQLiteStorage(storage.SQLiteConfig{OnDisk: true, Directory: "."})
	if err != nil {
		return nil, err
	}

	manager := gtfs.NewManager(s)

	headers, err := parseHeaders(staticHeaders)
	if err != nil {
		return nil, fmt.Errorf("invalid static header: %w", err)
	}

	shared, err := parseHeaders(sharedHeaders)
	if err != nil {
		return nil, fmt.Errorf("invalid header: %w", err)
	}

	for k, v := range shared {
		headers[k] = v
	}

	static, err := manager.LoadStaticAsync("cli", staticURL, headers, time.Now())
	if err != nil {
		err = manager.Refresh(context.Background())
		if err != nil {
			return nil, err
		}
		static, err = manager.LoadStaticAsync("cli", staticURL, nil, time.Now())
		if err != nil {
			return nil, err
		}
	}

	return static, nil
}

func LoadRealtimeFeed() (*gtfs.Realtime, error) {
	if realtimeURL == "" {
		return nil, fmt.Errorf("realtime URL is required")
	}

	static, err := LoadStaticFeed()
	if err != nil {
		return nil, fmt.Errorf("loading static feed: %w", err)
	}

	rh, err := parseHeaders(realtimeHeaders)
	if err != nil {
		return nil, fmt.Errorf("invalid realtime header: %w", err)
	}

	shared, err := parseHeaders(sharedHeaders)
	if err != nil {
		return nil, fmt.Errorf("invalid header: %w", err)
	}

	for k, v := range shared {
		rh[k] = v
	}

	fs, err := downloader.NewFilesystem("./gtfs-rt-cache.json")
	if err != nil {
		return nil, fmt.Errorf("creating realtime cache: %w", err)
	}

	s, err := storage.NewSQLiteStorage(storage.SQLiteConfig{OnDisk: true, Directory: "."})
	if err != nil {
		return nil, err
	}
	manager := gtfs.NewManager(s)
	manager.Downloader = fs

	realtime, err := manager.LoadRealtime("cli", static, realtimeURL, rh, time.Now())
	if err != nil {
		return nil, err
	}

	return realtime, nil
}
