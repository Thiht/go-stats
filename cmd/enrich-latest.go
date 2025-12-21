package cmd

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"

	"github.com/Thiht/go-command"
	"github.com/Thiht/go-stats/goproxy"
	"github.com/Thiht/go-stats/semver"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/schollz/progressbar/v3"
)

func EnrichLatestHandler(driver neo4j.DriverWithContext, goProxyClient goproxy.Client) command.Handler {
	return func(ctx context.Context, _ *flag.FlagSet, _ []string) int {
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

		writeSession := driver.NewSession(ctx, neo4j.SessionConfig{
			AccessMode: neo4j.AccessModeWrite,
		})
		defer writeSession.Close(ctx)

		result, err := readSession.Run(ctx, "MATCH (m:Module) RETURN m.name AS name", nil)
		if err != nil {
			slog.ErrorContext(ctx, "failed to list modules", slog.Any("error", err))
			return 1
		}

		chModules := make(chan string, 100)
		go func() {
			defer close(chModules)

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

		modulesSet := map[string]struct{}{}
		for name := range chModules {
			if _, exists := modulesSet[name]; exists {
				continue
			}
			modulesSet[name] = struct{}{}

			if err := progress.Add(1); err != nil {
				slog.WarnContext(ctx, "failed to update progress bar", slog.Any("error", err))
			}

			latestModuleInfo, err := getLatestModuleInfo(ctx, name, goProxyClient)
			if err != nil {
				slog.WarnContext(ctx, "failed to get latest module info", slog.String("module", name), slog.Any("error", err))
				continue
			}

			latestSemver, err := semver.Parse(latestModuleInfo.Version)
			if err != nil {
				slog.WarnContext(ctx, "failed to parse latest module version", slog.String("module", name), slog.String("version", latestModuleInfo.Version), slog.Any("error", err))
				continue
			}

			if _, err := writeSession.Run(ctx, `
				MATCH (m:Module { name: $name })
         		SET m += {
					latest: $latest,
					latestTime: date($latestTime),
					latestMajor: $latestMajor,
					latestMinor: $latestMinor,
					latestPatch: $latestPatch,
					latestLabel: $latestLabel
				}
			`,
				map[string]any{
					"name":        name,
					"latest":      latestModuleInfo.Version,
					"latestTime":  latestModuleInfo.Time.Format("2006-01-02"),
					"latestMajor": latestSemver.Major,
					"latestMinor": latestSemver.Minor,
					"latestPatch": latestSemver.Patch,
					"latestLabel": latestSemver.Label,
				}); err != nil {
				slog.ErrorContext(ctx, "failed to update module with latest info", slog.String("module", name), slog.Any("error", err))
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

	logger.Debug("getting latest module info from goproxy (cached)")
	latestModuleInfo, err := goProxyClient.GetModuleLatestInfo(ctx, m, true)
	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return goproxy.ModuleInfo{}, fmt.Errorf("timeout while getting latest module info: %w", err)
		}

		if !errors.Is(err, goproxy.ErrModuleNotFound) {
			return goproxy.ModuleInfo{}, fmt.Errorf("failed to get latest module info: %w", err)
		}

		logger.Debug("getting latest module info from goproxy")
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
