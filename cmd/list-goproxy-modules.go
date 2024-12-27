package cmd

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Thiht/go-command"
	"github.com/Thiht/go-stats/goproxy"
	"github.com/schollz/progressbar/v3"
)

func ListGoProxyModulesHandler(goProxyClient goproxy.Client) command.Handler {
	return func(ctx context.Context, flagSet *flag.FlagSet, _ []string) int {
		since, err := time.Parse(time.RFC3339, command.Lookup[string](flagSet, "since"))
		if err != nil {
			slog.Error("failed to parse \"since\"", slog.String("since", command.Lookup[string](flagSet, "since")), slog.Any("error", err))
			return 1
		}

		until, err := time.Parse(time.RFC3339, command.Lookup[string](flagSet, "until"))
		if err != nil {
			slog.Error("failed to parse \"until\"", slog.String("until", command.Lookup[string](flagSet, "until")), slog.Any("error", err))
			return 1
		}

		outputFile := command.Lookup[string](flagSet, "output-file")

		slog.Debug("opening output file", slog.String("file", outputFile))
		outputFileHandler, err := os.Create(outputFile)
		if err != nil {
			slog.Error("failed to open output file", slog.String("file", outputFile), slog.Any("error", err))
			return 1
		}
		defer outputFileHandler.Close()

		nbDays := int64(until.Sub(since).Hours() / 24)
		progress := progressbar.Default(nbDays, since.Format("2006-01-02"))

		chIndex := make(chan []goproxy.Index)
		go func() {
			defer close(chIndex)

			for {
				slog.Debug("listing index", slog.String("since", since.Format(time.RFC3339Nano)))
				index, err := goProxyClient.ListIndex(ctx, since)
				if err != nil {
					slog.Error("failed to list index", slog.Any("error", err))
					return
				}

				slog.Debug("received index", slog.Int("count", len(index)))

				since = index[len(index)-1].Timestamp
				chIndex <- index

				progress.Describe("Cursor: " + since.Format("2006-01-02"))
				if err := progress.Set64(nbDays - int64(until.Sub(since).Hours()/24)); err != nil {
					slog.Error("failed to update progress", slog.Any("error", err))
					return
				}

				if len(index) < goproxy.ListIndexMaxLimit {
					slog.Debug("no more index to list")
					break
				}

				if since.After(until) {
					slog.Debug("reached until date")
					break
				}

				time.Sleep(100 * time.Millisecond)
			}
		}()

		var modulesSet sync.Map
		for index := range chIndex {
			for _, i := range index {
				path := strings.ToLower(i.Path)

				if _, ok := modulesSet.Load(path); ok {
					continue
				}
				modulesSet.Store(path, struct{}{})

				if _, err := outputFileHandler.WriteString(path + " " + i.Version + "\n"); err != nil {
					slog.Error("failed to write module", slog.String("module", path), slog.Any("error", err))
					continue
				}
			}
		}

		return 0
	}
}
