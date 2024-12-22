package cmd

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Thiht/go-command"
	"github.com/Thiht/go-stats/goproxy"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/mod/module"
	"golang.org/x/sync/errgroup"
)

func ProcessModulesHandler(driver neo4j.DriverWithContext, goProxyClient goproxy.Client) command.Handler {
	return func(ctx context.Context, flagSet *flag.FlagSet, _ []string) int {
		parallel := command.Lookup[int](flagSet, "parallel")
		seedFile := command.Lookup[string](flagSet, "seed-file")

		initialModules, err := loadInitialModules(seedFile)
		if err != nil {
			slog.Error("failed to load initial modules", slog.Any("error", err))
			return 1
		}

		nbModules := int64(len(initialModules))
		var mxNbModules sync.Mutex

		g, gCtx := errgroup.WithContext(ctx)
		sem := make(chan struct{}, parallel)

		progress := progressbar.Default(nbModules)

		var pendingModules sync.Map
		chModules := make(chan module.Version, 1_000)
		go func() {
			for _, m := range initialModules {
				if _, loaded := pendingModules.LoadOrStore(m.Path, struct{}{}); loaded {
					mxNbModules.Lock()
					nbModules--
					progress.ChangeMax64(nbModules)
					mxNbModules.Unlock()
					continue
				}

				slog.Debug("adding module to processing queue", slog.String("module", m.Path))
				chModules <- m
			}

			slog.Debug("closing module channel")
		}()

		for m := range chModules {
			g.Go(func() error {
				sem <- struct{}{}
				defer func() {
					if err := progress.Add(1); err != nil {
						slog.Error("failed to update progress bar", slog.Any("error", err))
					}

					<-sem
				}()

				slog.Debug("processing module", slog.String("module", m.Path))

				dependencies, err := processModule(gCtx, m, goProxyClient, driver)
				if err != nil {
					slog.Error("failed to process module", slog.String("module", m.Path), slog.Any("error", err))
					return err
				}

				chDependencies := make(chan module.Version, len(dependencies))
				go func() {
					var loadedDependencies int64
					for dependency := range chDependencies {
						if _, loaded := pendingModules.LoadOrStore(dependency.Path, struct{}{}); !loaded {
							chModules <- dependency
							loadedDependencies++
						}
					}

					mxNbModules.Lock()
					nbModules += loadedDependencies
					progress.ChangeMax64(nbModules)
					mxNbModules.Unlock()
				}()

				for _, dependency := range dependencies {
					chDependencies <- dependency
				}

				close(chDependencies)

				slog.Debug("module processed", slog.String("module", m.Path))

				return nil
			})
		}

		if err := g.Wait(); err != nil {
			slog.Error("failed to process repositories", slog.Any("error", err))
			os.Exit(1)
		}

		// close(chModules)
		close(sem)

		return 0
	}
}

func loadInitialModules(seedFile string) ([]module.Version, error) {
	slog.Debug("opening seed file", slog.String("file", seedFile))
	seedFileHandler, err := os.Open(seedFile)
	if err != nil {
		slog.Error("failed to open seed file", slog.String("file", seedFile), slog.Any("error", err))
		return nil, fmt.Errorf("failed to open seed file: %w", err)
	}
	defer seedFileHandler.Close()

	slog.Debug("estimating seed file line count", slog.String("file", seedFile))
	file, err := os.Stat(seedFile)
	if err != nil {
		slog.Error("failed to stat seed file", slog.String("file", seedFile), slog.Any("error", err))
		return nil, fmt.Errorf("failed to stat seed file: %w", err)
	}

	estimatedCount := file.Size() / 35

	slog.Debug("reading seed file", slog.String("file", seedFile), slog.Int64("estimatedCount", estimatedCount))
	modules := make([]module.Version, 0, estimatedCount)
	scanner := bufio.NewScanner(seedFileHandler)
	for scanner.Scan() {
		modulePath := scanner.Text()
		modules = append(modules, module.Version{
			Path: strings.ToLower(modulePath),
		})
	}
	if err := scanner.Err(); err != nil {
		slog.Error("failed to read seed file", slog.String("file", seedFile), slog.Any("error", err))
		return nil, fmt.Errorf("failed to read seed file: %w", err)
	}

	slog.Debug("loaded initial modules", slog.Int("count", len(modules)), slog.Int64("estimatedCount", estimatedCount))

	return modules, nil
}

func processModule(ctx context.Context, modulePath module.Version, goProxyClient goproxy.Client, driver neo4j.DriverWithContext) ([]module.Version, error) {
	logger := slog.With(slog.Any("module", modulePath))

	if modulePath.Version == "" {
		logger.Debug("getting latest module info")
		moduleInfo, err := goProxyClient.GetModuleLatestInfo(ctx, modulePath.Path, true)
		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				logger.Error("timeout while getting latest module info", slog.Any("error", err), slog.Bool("cached", true))
				return nil, nil
			}

			if !errors.Is(err, goproxy.ErrModuleNotFound) {
				logger.Error("failed to get latest module info", slog.Any("error", err), slog.Bool("cached", true))
				return nil, nil
			}

			moduleInfo, err = goProxyClient.GetModuleLatestInfo(ctx, modulePath.Path, false)
			if err != nil {
				if errors.As(err, &netErr) && netErr.Timeout() {
					logger.Error("timeout while getting latest module info", slog.Any("error", err), slog.Bool("cached", false))
					return nil, nil
				}

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
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			logger.Error("timeout while getting module go.mod file", slog.Any("error", err), slog.Bool("cached", true))
			return nil, nil
		}

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
			if errors.As(err, &netErr) && netErr.Timeout() {
				logger.Error("timeout while getting module go.mod file", slog.Any("error", err), slog.Bool("cached", false))
				return nil, nil
			}

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

	dependencies := make([]map[string]any, 0, len(modFile.Require))
	dependsOn := make([]module.Version, 0, len(modFile.Require))

	for _, dependency := range modFile.Require {
		if dependency.Indirect {
			continue
		}

		dependency.Mod.Path = strings.ToLower(dependency.Mod.Path)
		dependsOn = append(dependsOn, dependency.Mod)

		dependencies = append(dependencies, map[string]any{
			"dependencyName":    dependency.Mod.Path,
			"dependencyVersion": dependency.Mod.Version,
			"dependencyOrg":     extractOrg(dependency.Mod.Path),
			"dependentName":     modFile.Module.Mod.Path,
			"dependentVersion":  modFile.Module.Mod.Version,
			"dependentOrg":      extractOrg(modFile.Module.Mod.Path),
		})
	}

	logger.Debug("creating module nodes and relationships for dependencies",
		slog.String("dependent", modFile.Module.Mod.Path),
		slog.String("dependentVersion", modFile.Module.Mod.Version),
		slog.Int("dependenciesCount", len(dependencies)))

	if _, err := neo4j.ExecuteQuery(ctx, driver, `
		UNWIND $dependencies AS dep
		MERGE (dependency:Module {name: dep.dependencyName, version: dep.dependencyVersion, org: dep.dependencyOrg})
		MERGE (dependent:Module {name: dep.dependentName, version: dep.dependentVersion, org: dep.dependentOrg})
		MERGE (dependent)-[:DEPENDS_ON]->(dependency)
		MERGE (dependency)-[:IS_DEPENDED_ON_BY]->(dependent)
		RETURN dependency, dependent
	`, map[string]any{
		"dependencies": dependencies,
	}, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase(""), neo4j.ExecuteQueryWithTransactionConfig(neo4j.WithTxTimeout(3*time.Second))); err != nil {
		logger.Error("failed to create module nodes and relationships for dependencies",
			slog.String("dependent", modFile.Module.Mod.Path),
			slog.String("dependentVersion", modFile.Module.Mod.Version),
			slog.Int("dependenciesCount", len(dependencies)),
			slog.Any("error", err))
		return nil, fmt.Errorf("failed to create module nodes and relationships: %w", err)
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
