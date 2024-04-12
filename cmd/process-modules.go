package cmd

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/Thiht/go-command"
	"github.com/Thiht/go-stats/goproxy"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"golang.org/x/mod/module"
	"golang.org/x/sync/errgroup"
)

func ProcessModulesHandler(driver neo4j.DriverWithContext, goProxyClient goproxy.Client) command.Handler {
	return func(ctx context.Context, flagSet *flag.FlagSet, _ []string) int {
		seedFile := command.Lookup[string](flagSet, "seed-file")

		slog.Debug("opening seed file", slog.String("file", seedFile))
		seedFileHandler, err := os.Open(seedFile)
		if err != nil {
			slog.Error("failed to open seed file", slog.String("file", seedFile), slog.Any("error", err))
			return 1
		}
		defer seedFileHandler.Close()

		slog.Debug("reading seed file", slog.String("file", seedFile))
		var modules []module.Version
		scanner := bufio.NewScanner(seedFileHandler)
		for scanner.Scan() {
			modulePath := scanner.Text()
			modules = append(modules, module.Version{
				Path: modulePath,
			})
		}
		if err := scanner.Err(); err != nil {
			slog.Error("failed to read seed file", slog.String("file", seedFile), slog.Any("error", err))
			return 1
		}

		g, gCtx := errgroup.WithContext(ctx)
		sem := make(chan struct{}, parallel)
		knownModules := sync.Map{}

		// TODO: rewrite this using channels instead of recursion
		processModules(gCtx, modules, &knownModules, g, sem, goProxyClient, driver)

		if err := g.Wait(); err != nil {
			slog.Error("failed to process repositories", slog.Any("error", err))
			os.Exit(1)
		}

		close(sem)

		return 0
	}
}

func processModules(ctx context.Context, modules []module.Version, knownModules *sync.Map, g *errgroup.Group, sem chan struct{}, goProxyClient goproxy.Client, driver neo4j.DriverWithContext) {
	for _, modulePath := range modules {
		logger := slog.With(slog.Any("module", modulePath))

		if _, loaded := knownModules.LoadOrStore(modulePath, struct{}{}); loaded {
			logger.Debug("module already processed", slog.Any("module", modulePath))
			continue
		}

		g.Go(func() error {
			sem <- struct{}{}
			defer func() {
				<-sem
			}()

			if modulePath.Version == "" {
				logger.Debug("getting latest module info")
				moduleInfo, err := goProxyClient.GetModuleLatestInfo(ctx, modulePath.Path)
				if err != nil {
					if errors.Is(err, goproxy.ErrModuleNotFound) {
						// This means the module is not depended on by any other module
						// It can happen with seeds because they sometimes contain multiple go.mod files and we process all of them for now
						logger.Warn("failed to get latest module info", slog.Any("error", err))
						return nil
					}

					logger.Error("failed to get latest module info", slog.Any("error", err))
					return fmt.Errorf("failed to get latest module info: %w", err)
				}

				modulePath.Version = moduleInfo.Version
			}

			modFile, err := goProxyClient.GetModuleModFile(ctx, modulePath.Path, modulePath.Version)
			if err != nil {
				if errors.Is(err, goproxy.ErrModuleNotFound) {
					// This means the module doesn't have a go.mod file
					logger.Warn("failed to get module go.mod file", slog.Any("error", err))
					return nil
				}

				logger.Error("failed to get module go.mod file", slog.Any("error", err))
				return fmt.Errorf("failed to get module go.mod file: %w", err)
			}

			if modFile.Module == nil {
				logger.Warn("go.mod file does not contain module information")
				return nil
			}

			logger.Debug("creating module node", slog.String("name", modFile.Module.Mod.Path), slog.String("version", modFile.Module.Mod.Version))
			if _, err := neo4j.ExecuteQuery(ctx, driver, "MERGE (m:Module {name: $name, version: $version}) RETURN m", map[string]any{
				"name":    modFile.Module.Mod.Path,
				"version": modFile.Module.Mod.Version,
			}, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("")); err != nil {
				logger.Error("failed to create module node", slog.String("name", modFile.Module.Mod.Path), slog.Any("error", err))
				return fmt.Errorf("failed to create module node: %w", err)
			}

			logger.Debug("processing direct dependencies")
			dependsOn := make([]module.Version, 0, len(modFile.Require))
			for _, dependency := range modFile.Require {
				if dependency.Indirect {
					continue
				}

				dependsOn = append(dependsOn, dependency.Mod)

				logger.Debug("creating dependency module node", slog.String("name", dependency.Mod.Path), slog.String("version", dependency.Mod.Version))
				if _, err := neo4j.ExecuteQuery(ctx, driver, "MERGE (m:Module {name: $name, version: $version}) RETURN m", map[string]any{
					"name":    dependency.Mod.Path,
					"version": dependency.Mod.Version,
				}, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("")); err != nil {
					logger.Error("failed to create dependency module node", slog.String("name", dependency.Mod.Path), slog.Any("error", err))
					return fmt.Errorf("failed to create dependency module node: %w", err)
				}

				logger.Debug("creating DEPENDS_ON relationship", slog.String("dependent", modFile.Module.Mod.Path), slog.String("dependentVersion", modFile.Module.Mod.Version), slog.String("dependency", dependency.Mod.Path), slog.String("dependencyVersion", dependency.Mod.Version))
				if _, err := neo4j.ExecuteQuery(ctx, driver, "MATCH (a:Module {name: $dependent, version: $dependentVersion}),(b:Module {name: $dependency, version: $dependencyVersion}) MERGE (a)-[r:DEPENDS_ON]->(b);", map[string]any{
					"dependent":         modFile.Module.Mod.Path,
					"dependentVersion":  modFile.Module.Mod.Version,
					"dependency":        dependency.Mod.Path,
					"dependencyVersion": dependency.Mod.Version,
				}, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("")); err != nil {
					logger.Error("failed to create DEPENDS_ON relationship", slog.String("dependent", modFile.Module.Mod.Path), slog.String("dependentVersion", modFile.Module.Mod.Version), slog.String("dependency", dependency.Mod.Path), slog.String("dependencyVersion", dependency.Mod.Version), slog.Any("error", err))
					return fmt.Errorf("failed to create DEPENDS_ON relationship: %w", err)
				}

				logger.Debug("creating IS_DEPENDED_ON_BY relationship", slog.String("dependent", modFile.Module.Mod.Path), slog.String("dependentVersion", modFile.Module.Mod.Version), slog.String("dependency", dependency.Mod.Path), slog.String("dependencyVersion", dependency.Mod.Version))
				if _, err := neo4j.ExecuteQuery(ctx, driver, "MATCH (a:Module {name: $dependent, version: $dependentVersion}),(b:Module {name: $dependency, version: $dependencyVersion}) MERGE (b)-[r:IS_DEPENDED_ON_BY]->(a);", map[string]any{
					"dependent":         modFile.Module.Mod.Path,
					"dependentVersion":  modFile.Module.Mod.Version,
					"dependency":        dependency.Mod.Path,
					"dependencyVersion": dependency.Mod.Version,
				}, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("")); err != nil {
					logger.Error("failed to create IS_DEPENDED_ON_BY relationship", slog.String("dependent", modFile.Module.Mod.Path), slog.String("dependentVersion", modFile.Module.Mod.Version), slog.String("dependency", dependency.Mod.Path), slog.String("dependencyVersion", dependency.Mod.Version), slog.Any("error", err))
					return fmt.Errorf("failed to create IS_DEPENDED_ON_BY relationship: %w", err)
				}
			}

			processModules(ctx, dependsOn, knownModules, g, sem, goProxyClient, driver)

			return nil
		})
	}
}
