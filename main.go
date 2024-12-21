package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/Thiht/go-command"
	"github.com/Thiht/go-stats/cmd"
	"github.com/Thiht/go-stats/goproxy"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func main() {
	ctx := context.Background()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	driver, err := setupNeo4j(ctx)
	if err != nil {
		slog.Error("failed to setup neo4j", slog.Any("error", err))
		os.Exit(1)
	}
	defer driver.Close(ctx)

	goProxyClient := goproxy.NewGoProxyClient()

	root := command.Root()
	root.SubCommand("repositories-to-modules").Action(cmd.RepositoriesToModulesHandler()).Flags(func(flagSet *flag.FlagSet) {
	root.SubCommand("list-goproxy-modules").Action(cmd.ListGoProxyModulesHandler(goProxyClient)).Flags(func(flagSet *flag.FlagSet) {
		flagSet.String("since", "2019-04-10T19:08:52.997264Z", "List modules since this date")
		flagSet.String("output-file", "./data/go-proxy-modules.txt", "Output file containing the list of Go module paths")
	})
	root.SubCommand("process-modules").Action(cmd.ProcessModulesHandler(driver, goProxyClient)).Flags(func(flagSet *flag.FlagSet) {
		flagSet.String("seed-file", "", "")
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
	if _, err := session.Run(ctx, "CREATE INDEX IF NOT EXISTS FOR (m:Module) ON (m.name);", nil); err != nil {
		slog.Error("failed to create index on :Module(name)", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create index on :Module(name): %w", err)
	}

	if _, err := session.Run(ctx, "CREATE INDEX IF NOT EXISTS FOR (m:Module) ON (m.version);", nil); err != nil {
		slog.Error("failed to create index on :Module(version)", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create index on :Module(version): %w", err)
	}

	return driver, nil
}
