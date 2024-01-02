package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"tidbyt.dev/gtfs"
	"tidbyt.dev/gtfs/storage"
)

var rootCmd = &cobra.Command{
	Use:          "gtfs",
	Short:        "Tidbyt GTFS tool",
	Long:         "Does stuff with GTFS data",
	SilenceUsage: true,
}

var (
	staticURL   string
	realtimeURL string
)

func init() {
	rootCmd.PersistentFlags().StringVarP(&staticURL, "static", "", "", "GTFS Static URL")
	rootCmd.PersistentFlags().StringVarP(&realtimeURL, "realtime", "", "", "GTFS Realtime URL")
	rootCmd.AddCommand(departuresCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
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

	static, err := manager.LoadStaticAsync("cli", staticURL, nil, time.Now())
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
	if staticURL == "" {
		return nil, fmt.Errorf("static URL is required")
	}

	s, err := storage.NewSQLiteStorage(storage.SQLiteConfig{OnDisk: true, Directory: "."})
	if err != nil {
		return nil, err
	}
	manager := gtfs.NewManager(s)

	realtime, err := manager.LoadRealtime("cli", staticURL, nil, realtimeURL, nil, time.Now())
	if err != nil {
		return nil, err
	}

	return realtime, nil
}
