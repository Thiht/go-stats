package cmd

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"

	"github.com/Thiht/go-command"
	"github.com/Thiht/go-stats/goproxy"
	"github.com/cenkalti/backoff/v5"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/sync/errgroup"
)

func ListGoProxyLatestHandler(driver neo4j.DriverWithContext, goProxyClient goproxy.Client) command.Handler {
	return func(ctx context.Context, flagSet *flag.FlagSet, _ []string) int {
		parallel := command.Lookup[int](flagSet, "parallel")
		outputFile := command.Lookup[string](flagSet, "output-file")

		slog.InfoContext(ctx, "opening output file", slog.String("file", outputFile))
		outputFileHandler, err := os.Create(outputFile)
		if err != nil {
			slog.ErrorContext(ctx, "failed to open output file", slog.String("file", outputFile), slog.Any("error", err))
			return 1
		}
		defer outputFileHandler.Close()

		bufferedWriter := bufio.NewWriter(outputFileHandler)
		defer bufferedWriter.Flush()

		csvWriter := csv.NewWriter(bufferedWriter)
		defer csvWriter.Flush()

		csvWriter.Write([]string{"module", "latest"})

		slog.InfoContext(ctx, "counting distinct modules")
		nbModules, err := countDistinctModules(ctx, driver)
		if err != nil {
			slog.ErrorContext(ctx, "failed to count distinct modules", slog.Any("error", err))
			return 1
		}

		progress := progressbar.Default(nbModules)

		readSession := driver.NewSession(ctx, neo4j.SessionConfig{
			AccessMode: neo4j.AccessModeRead,
		})
		defer readSession.Close(ctx)

		result, err := readSession.Run(ctx, "MATCH (m:Module) RETURN m.name AS name", nil)
		if err != nil {
			slog.ErrorContext(ctx, "failed to list modules", slog.Any("error", err))
			return 1
		}

		chModules := make(chan string, parallel)
		go func() {
			defer close(chModules)

			ctx := context.Background()

			for result.Next(ctx) {
				record := result.Record()

				rawName, ok := record.Get("name")
				if !ok {
					slog.WarnContext(ctx, "failed to get module name from record")
					continue
				}

				name, ok := rawName.(string)
				if !ok {
					slog.WarnContext(ctx, "module name is not a string", slog.Any("name", rawName))
					continue
				}

				chModules <- name
			}
			if err := result.Err(); err != nil {
				slog.ErrorContext(ctx, "error while iterating module records", slog.Any("error", err))
			}
		}()

		type moduleResult struct {
			name    string
			version string
		}

		modulesSet := map[string]struct{}{}
		chResults := make(chan moduleResult, parallel)
		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(parallel)
		go func() {
			defer close(chResults)

			for name := range chModules {
				if _, exists := modulesSet[name]; exists {
					continue
				}
				modulesSet[name] = struct{}{}

				g.Go(func() error {
					latestModuleInfo, err := getLatestModuleInfo(gCtx, name, goProxyClient)
					if err != nil {
						if !errors.Is(err, goproxy.ErrModuleNotFound) {
							slog.WarnContext(gCtx, "failed to get latest module info", slog.String("module", name), slog.Any("error", err))
						}

						return nil
					}

					chResults <- moduleResult{name: name, version: latestModuleInfo.Version}

					return nil
				})
			}

			if err := g.Wait(); err != nil {
				slog.ErrorContext(gCtx, "error while processing modules", slog.Any("error", err))
			}
		}()

		for res := range chResults {
			if err := progress.Add(1); err != nil {
				slog.WarnContext(ctx, "failed to update progress bar", slog.Any("error", err))
			}

			if err := csvWriter.Write([]string{res.name, res.version}); err != nil {
				slog.ErrorContext(ctx, "failed to write module latest info", slog.String("module", res.name), slog.String("latest", res.version), slog.Any("error", err))
				continue
			}
		}

		return 0
	}
}

func countDistinctModules(ctx context.Context, driver neo4j.DriverWithContext) (int64, error) {
	result, err := neo4j.ExecuteQuery(ctx, driver, `
		MATCH (m:Module)
		RETURN COUNT(DISTINCT m.name) AS nbModules
	`, nil, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase(""))
	if err != nil {
		return 0, fmt.Errorf("failed to count distinct module names: %w", err)
	}

	if len(result.Records) == 0 {
		return 0, fmt.Errorf("no records returned from count distinct modules query")
	}

	rawNbModules, ok := result.Records[0].Get("nbModules")
	if !ok {
		return 0, fmt.Errorf("failed to get nbModules from record")
	}

	nbModules, ok := rawNbModules.(int64)
	if !ok {
		return 0, fmt.Errorf("nbModules is not an int64: %v", rawNbModules)
	}

	return nbModules, nil
}

func getLatestModuleInfo(ctx context.Context, m string, goProxyClient goproxy.Client) (goproxy.ModuleInfo, error) {
	logger := slog.With(slog.String("module", m))

	logger.DebugContext(ctx, "getting latest module info from goproxy (cached)")

	latestModuleInfo, err := backoff.Retry(ctx, func() (goproxy.ModuleInfo, error) {
		res, err := goProxyClient.GetModuleLatestInfo(ctx, m, true)
		if errors.Is(err, goproxy.ErrModuleNotFound) {
			return goproxy.ModuleInfo{}, backoff.Permanent(err)
		}

		return res, nil
	}, backoff.WithBackOff(backoff.NewExponentialBackOff()), backoff.WithMaxTries(3))

	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return goproxy.ModuleInfo{}, fmt.Errorf("timeout while getting latest module info: %w", err)
		}

		if !errors.Is(err, goproxy.ErrModuleNotFound) {
			return goproxy.ModuleInfo{}, fmt.Errorf("failed to get latest module info: %w", err)
		}

		logger.DebugContext(ctx, "getting latest module info from goproxy")
		latestModuleInfo, err = goProxyClient.GetModuleLatestInfo(ctx, m, false)
		if err != nil {
			if errors.As(err, &netErr) && netErr.Timeout() {
				return goproxy.ModuleInfo{}, fmt.Errorf("timeout while getting latest module info: %w", err)
			}

			if errors.Is(err, goproxy.ErrModuleNotFound) {
				return goproxy.ModuleInfo{}, fmt.Errorf("latest module info not found: %w", err)
			}

			return goproxy.ModuleInfo{}, fmt.Errorf("failed to get latest module info: %w", err)
		}
	}

	return latestModuleInfo, nil
}
