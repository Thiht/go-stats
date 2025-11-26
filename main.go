package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"time"

	"github.com/Thiht/go-command"
	"github.com/Thiht/go-stats/cmd"
	"github.com/Thiht/go-stats/goproxy"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func main() {
	ctx := context.Background()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))

	driver, err := setupNeo4j(ctx)
	if err != nil {
		slog.Error("failed to setup neo4j", slog.Any("error", err))
		os.Exit(1)
	}
	defer driver.Close(ctx)

	goProxyClient := goproxy.NewGoProxyClient()

	root := command.Root().Flags(func(flagSet *flag.FlagSet) {
		flagSet.String("log-level", "warn", "Log level (debug, info, warn, error)")
	}).Middlewares(func(next command.Handler) command.Handler {
		return func(ctx context.Context, flagSet *flag.FlagSet, args []string) int {
			var level slog.Level
			if err := level.UnmarshalText([]byte(command.Lookup[string](flagSet, "log-level"))); err != nil {
				slog.Error("invalid log level, fallback to warn", slog.Any("error", err))
				level = slog.LevelWarn
			}

			slog.SetLogLoggerLevel(level)

			return next(ctx, flagSet, args)
		}
	})
	root.SubCommand("repositories-to-modules").Action(cmd.RepositoriesToModulesHandler()).Flags(func(flagSet *flag.FlagSet) {
		flagSet.String("input-file", "./data/seed.txt", "File containing a list of Go repositories to convert to Go module paths")
		flagSet.String("output-file", "./data/seed-modules.txt", "Output file containing the list of Go module paths")
	})
	root.SubCommand("list-goproxy-modules").Action(cmd.ListGoProxyModulesHandler(goProxyClient)).Flags(func(flagSet *flag.FlagSet) {
		flagSet.String("since", "2019-04-10T19:08:52.997264Z", "List modules since this date")
		flagSet.String("until", time.Now().Format(time.RFC3339Nano), "List modules until this date")
		flagSet.String("output-file", "./data/go-proxy-modules.txt", "Output file containing the list of Go module paths")
	})
	root.SubCommand("process-modules").Action(cmd.ProcessModulesHandler(driver, goProxyClient)).Flags(func(flagSet *flag.FlagSet) {
		flagSet.Int("parallel", runtime.NumCPU(), "Number of parallel workers")
		flagSet.String("seed-file", "", "")
		flagSet.Int64("offset", 0, "Number of lines to skip from the seed files")
	})
	root.Execute(ctx)
}

func setupNeo4j(ctx context.Context) (neo4j.DriverWithContext, error) {
	slog.Debug("creating neo4j driver")
	driver, err := neo4j.NewDriverWithContext("neo4j://localhost", neo4j.NoAuth())
	if err != nil {
		slog.Error("failed to create neo4j driver", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create neo4j driver: %w", err)
	}

	slog.Debug("verifying neo4j driver connectivity")
	if err := driver.VerifyConnectivity(ctx); err != nil {
		slog.Error("failed to verify neo4j driver connectivity", slog.Any("error", err))
		return nil, fmt.Errorf("failed to verify neo4j driver connectivity: %w", err)
	}

	slog.Debug("creating neo4j session")
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: ""})
	defer session.Close(ctx)

	slog.Debug("creating neo4j indexes")
	if _, err := session.Run(ctx, `
		CREATE CONSTRAINT module_identity IF NOT EXISTS
		FOR (m:Module)
		REQUIRE (m.name, m.version) IS UNIQUE
	`, nil); err != nil {
		slog.Error("failed to create module_identity constraint", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create module_identity constraint: %w", err)
	}

	if _, err := session.Run(ctx, "CREATE INDEX module_version_latest IF NOT EXISTS FOR (m:Module) ON (m.version, m.latest)", nil); err != nil {
		slog.Error("failed to create module_version_latest index", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create module_version_latest index: %w", err)
	}

	return driver, nil
}
