package main

import (
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
	feedURL string
)

func init() {
	rootCmd.PersistentFlags().StringVarP(&feedURL, "url", "u", "", "GTFS feed URL")
	rootCmd.MarkFlagRequired("url")
	rootCmd.AddCommand(departuresCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func LoadStaticFeed(url string) (*gtfs.Static, error) {
	s, err := storage.NewSQLiteStorage(storage.SQLiteConfig{OnDisk: true, Directory: "."})
	if err != nil {
		return nil, err
	}
	manager := gtfs.NewManager(s)

	static, err := manager.LoadStatic(url, time.Now())
	if err != nil {
		return nil, err
	}

	return static, nil
}
