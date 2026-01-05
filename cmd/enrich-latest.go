package cmd

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/Thiht/go-command"
	"github.com/Thiht/go-stats/goproxy"
	"github.com/Thiht/go-stats/semver"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/schollz/progressbar/v3"
)

func EnrichLatestHandler(driver neo4j.DriverWithContext, goProxyClient goproxy.Client) command.Handler {
	return func(ctx context.Context, flagSet *flag.FlagSet, _ []string) int {
		inputFile := command.Lookup[string](flagSet, "input-file")

		slog.DebugContext(ctx, "opening input file", slog.String("file", inputFile))
		inputFileHandler, err := os.Open(inputFile)
		if err != nil {
			slog.ErrorContext(ctx, "failed to open input file", slog.String("file", inputFile), slog.Any("error", err))
			return 1
		}
		defer inputFileHandler.Close()

		reader := csv.NewReader(inputFileHandler)
		reader.FieldsPerRecord = 2

		// Skip header
		if _, err := reader.Read(); err != nil && !errors.Is(err, io.EOF) {
			slog.ErrorContext(ctx, "failed to read header", slog.String("file", inputFile), slog.Any("error", err))
			return 1
		}

		records := [][]string{}
		for {
			record, err := reader.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				slog.ErrorContext(ctx, "failed to read csv record", slog.Any("error", err))
				return 1
			}
			records = append(records, record)
		}

		progress := progressbar.Default(int64(len(records)))

		session := driver.NewSession(ctx, neo4j.SessionConfig{
			AccessMode: neo4j.AccessModeWrite,
		})
		defer session.Close(ctx)

		type moduleUpdate struct {
			name        string
			latest      string
			latestMajor string
			latestMinor string
			latestPatch string
			latestLabel string
		}

		const batchSize = 1_000
		batch := make([]moduleUpdate, 0, batchSize)

		flushBatch := func() error {
			if len(batch) == 0 {
				return nil
			}

			updates := make([]map[string]any, len(batch))
			for i, update := range batch {
				updates[i] = map[string]any{
					"name":        update.name,
					"latest":      update.latest,
					"latestMajor": update.latestMajor,
					"latestMinor": update.latestMinor,
					"latestPatch": update.latestPatch,
					"latestLabel": update.latestLabel,
				}
			}

			if _, err := session.Run(ctx, `
				UNWIND $updates AS update
				MATCH (m:Module { name: update.name })
				SET m += {
					isLatest: m.version = update.latest,
					latest: update.latest,
				    latestMajor: update.latestMajor,
				    latestMinor: update.latestMinor,
				    latestPatch: update.latestPatch,
				    latestLabel: update.latestLabel
				}
			`, map[string]any{"updates": updates}); err != nil {
				return fmt.Errorf("failed to batch update Module nodes: %w", err)
			}

			batch = batch[:0]
			return nil
		}

		for _, record := range records {
			moduleName := record[0]
			latestVersion := record[1]

			if err := progress.Add(1); err != nil {
				slog.WarnContext(ctx, "failed to update progress bar", slog.Any("error", err))
			}

			latestSemver, err := semver.Parse(latestVersion)
			if err != nil {
				slog.WarnContext(ctx, "failed to parse latest module version", slog.String("module", moduleName), slog.String("version", latestVersion), slog.Any("error", err))
				continue
			}

			batch = append(batch, moduleUpdate{
				name:        moduleName,
				latest:      latestVersion,
				latestMajor: latestSemver.Major,
				latestMinor: latestSemver.Minor,
				latestPatch: latestSemver.Patch,
				latestLabel: latestSemver.Label,
			})

			if len(batch) >= batchSize {
				if err := flushBatch(); err != nil {
					slog.ErrorContext(ctx, "failed to flush batch", slog.Any("error", err))
					return 1
				}
			}
		}

		if err := flushBatch(); err != nil {
			slog.ErrorContext(ctx, "failed to flush final batch", slog.Any("error", err))
			return 1
		}

		return 0
	}
}
