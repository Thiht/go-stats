package cmd

import (
	"bufio"
	"context"
	"encoding/csv"
	"flag"
	"log/slog"
	"os"
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

		bufferedWriter := bufio.NewWriter(outputFileHandler)
		defer bufferedWriter.Flush()

		csvWriter := csv.NewWriter(bufferedWriter)
		defer csvWriter.Flush()

		csvWriter.Write([]string{"timestamp", "module", "version"})

		nbDays := int64(until.Sub(since).Hours() / 24)
		progress := progressbar.Default(nbDays, since.Format("2006-01-02"))

		chIndex := make(chan goproxy.Index, goproxy.ListIndexMaxLimit)
		go func() {
			defer close(chIndex)

			for index, err := range goProxyClient.IterIndex(ctx, since) {
				if err != nil {
					slog.Error("failed to list index", slog.Any("error", err))
					return
				}

				chIndex <- index.Index
				progress.Describe("Cursor: " + index.Current.Format("2006-01-02"))
				if err := progress.Set64(nbDays - int64(until.Sub(index.Current).Hours()/24)); err != nil {
					slog.Error("failed to update progress", slog.Any("error", err))
					return
				}

				if index.Current.After(until) {
					slog.Debug("reached until date")
					break
				}
			}
		}()

		modulesSet := map[string]struct{}{}
		for i := range chIndex {
			key := i.Path + "@" + i.Version
			if _, exists := modulesSet[key]; exists {
				continue
			}
			modulesSet[key] = struct{}{}

			if err := csvWriter.Write([]string{i.Timestamp.Format(time.RFC3339Nano), i.Path, i.Version}); err != nil {
				slog.Error("failed to write module", slog.String("timestamp", i.Timestamp.Format(time.RFC3339Nano)), slog.String("module", i.Path), slog.String("version", i.Version), slog.Any("error", err))
				continue
			}
		}

		return 0
	}
}
