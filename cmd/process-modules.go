package cmd

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
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

		slog.Debug("estimating seed file line count", slog.String("file", seedFile))
		file, err := os.Stat(seedFile)
		if err != nil {
			slog.Error("failed to stat seed file", slog.String("file", seedFile), slog.Any("error", err))
			return 1
		}

		nbLines := file.Size() / 35

		slog.Debug("reading seed file", slog.String("file", seedFile), slog.Int64("estimatedNbLines", nbLines))
		modules := make([]module.Version, 0, nbLines)
		var pendingModules sync.Map
		scanner := bufio.NewScanner(seedFileHandler)
		for scanner.Scan() {
			modulePath := scanner.Text()
			modules = append(modules, module.Version{
				Path: strings.ToLower(modulePath),
			})
			pendingModules.Store(modulePath, struct{}{})
		}
		if err := scanner.Err(); err != nil {
			slog.Error("failed to read seed file", slog.String("file", seedFile), slog.Any("error", err))
			return 1
		}

		g, gCtx := errgroup.WithContext(ctx)
		sem := make(chan struct{}, parallel)
		var knownModules sync.Map
		var mxModules sync.RWMutex

		for i := 0; ; i++ {
			mxModules.RLock()
			if i >= len(modules) {
				mxModules.RUnlock()
				break
			}

			module := modules[i]
			mxModules.RUnlock()

			if _, loaded := knownModules.LoadOrStore(module, struct{}{}); loaded {
				continue
			}

			g.Go(func() error {
				sem <- struct{}{}
				defer func() {
					<-sem
				}()

				dependencies, err := processModule(gCtx, module, goProxyClient, driver)
				if err != nil {
					return err
				}

				mxModules.Lock()
				for _, dependency := range dependencies {
					// Only append the dependency if it's not already in the pending list
					if _, loaded := pendingModules.LoadOrStore(dependency.Path, struct{}{}); !loaded {
						modules = append(modules, dependency)
					}
				}
				mxModules.Unlock()

				return nil
			})
		}

		if err := g.Wait(); err != nil {
			slog.Error("failed to process repositories", slog.Any("error", err))
			os.Exit(1)
		}

		close(sem)

		return 0
	}
}

func processModule(ctx context.Context, modulePath module.Version, goProxyClient goproxy.Client, driver neo4j.DriverWithContext) ([]module.Version, error) {
	logger := slog.With(slog.Any("module", modulePath))

	if modulePath.Version == "" {
		logger.Debug("getting latest module info")
		moduleInfo, err := goProxyClient.GetModuleLatestInfo(ctx, modulePath.Path, true)
		if err != nil {
			if !errors.Is(err, goproxy.ErrModuleNotFound) {
				logger.Error("failed to get latest module info", slog.Any("error", err), slog.Bool("cached", true))
				return nil, nil
			}

			moduleInfo, err = goProxyClient.GetModuleLatestInfo(ctx, modulePath.Path, false)
			if err != nil {
				if errors.Is(err, goproxy.ErrModuleNotFound) {
					// This means the module is not depended on by any other module
					// It can happen with seeds because they sometimes contain multiple go.mod files and we process all of them for now
					logger.Warn("latest module info not found", slog.Any("error", err))
					return nil, nil
				}

				logger.Error("failed to get latest module info", slog.Any("error", err), slog.Bool("cached", false))
				return nil, nil
			}
		}

		modulePath.Version = moduleInfo.Version
	}

	modFile, err := goProxyClient.GetModuleModFile(ctx, modulePath.Path, modulePath.Version, true)
	if err != nil {
		if errors.Is(err, goproxy.ErrInvalidModFile) {
			logger.Warn("invalid go.mod file", slog.Any("error", err))
			return nil, nil
		}

		if !errors.Is(err, goproxy.ErrModuleNotFound) {
			logger.Error("failed to get module go.mod file", slog.Any("error", err), slog.Bool("cached", true))
			return nil, nil
		}

		modFile, err = goProxyClient.GetModuleModFile(ctx, modulePath.Path, modulePath.Version, false)
		if err != nil {
			if errors.Is(err, goproxy.ErrInvalidModFile) {
				logger.Warn("invalid go.mod file", slog.Any("error", err))
				return nil, nil
			}

			if errors.Is(err, goproxy.ErrModuleNotFound) {
				// This means the module doesn't have a go.mod file
				logger.Warn("module go.mod file not found", slog.Any("error", err))
				return nil, nil
			}

			logger.Error("failed to get module go.mod file", slog.Any("error", err), slog.Bool("cached", false))
			return nil, nil
		}
	}

	if modFile.Module == nil {
		logger.Warn("go.mod file does not contain module information")
		return nil, nil
	}

	logger.Debug("creating module node", slog.String("name", modFile.Module.Mod.Path), slog.String("version", modFile.Module.Mod.Version))
	if _, err := neo4j.ExecuteQuery(ctx, driver, "MERGE (m:Module {name: $name, version: $version}) RETURN m", map[string]any{
		"name":    modFile.Module.Mod.Path,
		"version": modFile.Module.Mod.Version,
	}, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("")); err != nil {
		logger.Error("failed to create module node", slog.String("name", modFile.Module.Mod.Path), slog.Any("error", err))
		return nil, fmt.Errorf("failed to create module node: %w", err)
	}

	logger.Debug("processing direct dependencies")
	dependsOn := make([]module.Version, 0, len(modFile.Require))
	for _, dependency := range modFile.Require {
		if dependency.Indirect {
			continue
		}

		dependency.Mod.Path = strings.ToLower(dependency.Mod.Path)
		dependsOn = append(dependsOn, dependency.Mod)

		logger.Debug("creating dependency module node", slog.String("name", dependency.Mod.Path), slog.String("version", dependency.Mod.Version))
		if _, err := neo4j.ExecuteQuery(ctx, driver, "MERGE (m:Module {name: $name, version: $version, org: $org}) RETURN m", map[string]any{
			"name":    dependency.Mod.Path,
			"version": dependency.Mod.Version,
			"org":     extractOrg(dependency.Mod.Path),
		}, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("")); err != nil {
			logger.Error("failed to create dependency module node", slog.String("name", dependency.Mod.Path), slog.Any("error", err))
			return nil, fmt.Errorf("failed to create dependency module node: %w", err)
		}

		logger.Debug("creating DEPENDS_ON relationship", slog.String("dependent", modFile.Module.Mod.Path), slog.String("dependentVersion", modFile.Module.Mod.Version), slog.String("dependency", dependency.Mod.Path), slog.String("dependencyVersion", dependency.Mod.Version))
		if _, err := neo4j.ExecuteQuery(ctx, driver, "MATCH (a:Module {name: $dependent, version: $dependentVersion}),(b:Module {name: $dependency, version: $dependencyVersion}) MERGE (a)-[r:DEPENDS_ON]->(b);", map[string]any{
			"dependent":         modFile.Module.Mod.Path,
			"dependentVersion":  modFile.Module.Mod.Version,
			"dependency":        dependency.Mod.Path,
			"dependencyVersion": dependency.Mod.Version,
		}, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("")); err != nil {
			logger.Error("failed to create DEPENDS_ON relationship", slog.String("dependent", modFile.Module.Mod.Path), slog.String("dependentVersion", modFile.Module.Mod.Version), slog.String("dependency", dependency.Mod.Path), slog.String("dependencyVersion", dependency.Mod.Version), slog.Any("error", err))
			return nil, fmt.Errorf("failed to create DEPENDS_ON relationship: %w", err)
		}

		logger.Debug("creating IS_DEPENDED_ON_BY relationship", slog.String("dependent", modFile.Module.Mod.Path), slog.String("dependentVersion", modFile.Module.Mod.Version), slog.String("dependency", dependency.Mod.Path), slog.String("dependencyVersion", dependency.Mod.Version))
		if _, err := neo4j.ExecuteQuery(ctx, driver, "MATCH (a:Module {name: $dependent, version: $dependentVersion}),(b:Module {name: $dependency, version: $dependencyVersion}) MERGE (b)-[r:IS_DEPENDED_ON_BY]->(a);", map[string]any{
			"dependent":         modFile.Module.Mod.Path,
			"dependentVersion":  modFile.Module.Mod.Version,
			"dependency":        dependency.Mod.Path,
			"dependencyVersion": dependency.Mod.Version,
		}, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("")); err != nil {
			logger.Error("failed to create IS_DEPENDED_ON_BY relationship", slog.String("dependent", modFile.Module.Mod.Path), slog.String("dependentVersion", modFile.Module.Mod.Version), slog.String("dependency", dependency.Mod.Path), slog.String("dependencyVersion", dependency.Mod.Version), slog.Any("error", err))
			return nil, fmt.Errorf("failed to create IS_DEPENDED_ON_BY relationship: %w", err)
		}
	}

	return dependsOn, nil
}

func extractOrg(modulePath string) string {
	switch {
	case strings.HasPrefix(modulePath, "github.com/"):
		return strings.Split(modulePath, "/")[1]

	case strings.HasPrefix(modulePath, "google.golang.org/"):
		return "google"

	case strings.HasPrefix(modulePath, "golang.org/"),
		strings.HasPrefix(modulePath, "github.com/pkg/"):
		return "golang"

	case strings.HasPrefix(modulePath, "k8s.io/"),
		strings.HasPrefix(modulePath, "sigs.k8s.io/"):
		return "kubernetes"

	case strings.HasPrefix(modulePath, "go.uber.org/"):
		return "uber-go"

	case strings.HasPrefix(modulePath, "gorm.io/gorm"):
		return "go-gorm"

	case strings.HasPrefix(modulePath, "go.opentelemetry.io/"):
		return "open-telemetry"

	case strings.HasPrefix(modulePath, "go.mongodb.org/"):
		return "mongodb"
	}

	return ""
}
