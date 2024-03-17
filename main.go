package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-git/go-git/v5"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"golang.org/x/mod/modfile"
	"golang.org/x/sync/errgroup"
)

const (
	seedFile = "./data/top100.txt"
	parallel = 10
)

type Module struct {
	URL     string
	Module  string
	Version string
}

func main() {
	ctx := context.Background()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	goProxyClient := NewGoProxyClient()

	driver, err := setupNeo4j(ctx)
	if err != nil {
		slog.Error("failed to setup neo4j", slog.Any("error", err))
		os.Exit(1)
	}
	defer driver.Close(ctx)

	modules, err := listSeedModules(ctx, seedFile)
	if err != nil {
		slog.Error("failed to list seed modules", slog.Any("error", err))
		os.Exit(1)
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

	slog.Info("success")
}

func processModules(ctx context.Context, modules []Module, knownModules *sync.Map, g *errgroup.Group, sem chan struct{}, goProxyClient GoProxyClient, driver neo4j.DriverWithContext) {
	for _, module := range modules {
		logger := slog.With(slog.Any("module", module))

		if _, ok := knownModules.Load(module); ok {
			logger.Debug("module already processed", slog.Any("module", module))
			continue
		}
		knownModules.Store(module, struct{}{})

		g.Go(func() error {
			sem <- struct{}{}
			defer func() {
				<-sem
			}()

			if module.Version == "" {
				logger.Debug("getting latest module info")
				moduleInfo, err := goProxyClient.GetModuleLatestInfo(ctx, module.Module)
				if err != nil {
					if errors.Is(err, ErrModuleNotFound) {
						// This means the module is not depended on by any other module
						// It can happen with seeds because they sometimes contain multiple go.mod files and we process all of them for now
						logger.Warn("module not found")
						return nil
					}

					logger.Error("failed to get latest module info", slog.Any("error", err))
					return err
				}

				module.Version = moduleInfo.Version
			}

			modFile, err := goProxyClient.GetModuleModFile(ctx, module.Module, module.Version)
			if err != nil {
				if errors.Is(err, ErrModuleNotFound) {
					// This means the module doesn't have a go.mod file
					logger.Warn("module not found")
					return nil
				}

				logger.Error("failed to get module go.mod file", slog.Any("error", err))
				return err
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
				return err
			}

			logger.Debug("processing direct dependencies")
			dependsOn := make([]Module, 0, len(modFile.Require))
			for _, dependency := range modFile.Require {
				if dependency.Indirect {
					continue
				}

				dependsOn = append(dependsOn, Module{
					Module:  dependency.Mod.Path,
					Version: dependency.Mod.Version,
				})

				logger.Debug("creating dependency module node", slog.String("name", dependency.Mod.Path), slog.String("version", dependency.Mod.Version))
				if _, err := neo4j.ExecuteQuery(ctx, driver, "MERGE (m:Module {name: $name, version: $version}) RETURN m", map[string]any{
					"name":    dependency.Mod.Path,
					"version": dependency.Mod.Version,
				}, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("")); err != nil {
					logger.Error("failed to create dependency module node", slog.String("name", dependency.Mod.Path), slog.Any("error", err))
					return err
				}

				logger.Debug("creating DEPENDS_ON relationship", slog.String("dependent", modFile.Module.Mod.Path), slog.String("dependentVersion", modFile.Module.Mod.Version), slog.String("dependency", dependency.Mod.Path), slog.String("dependencyVersion", dependency.Mod.Version))
				if _, err := neo4j.ExecuteQuery(ctx, driver, "MATCH (a:Module {name: $dependent, version: $dependentVersion}),(b:Module {name: $dependency, version: $dependencyVersion}) MERGE (a)-[r:DEPENDS_ON]->(b);", map[string]any{
					"dependent":         modFile.Module.Mod.Path,
					"dependentVersion":  modFile.Module.Mod.Version,
					"dependency":        dependency.Mod.Path,
					"dependencyVersion": dependency.Mod.Version,
				}, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("")); err != nil {
					logger.Error("failed to create DEPENDS_ON relationship", slog.String("dependent", modFile.Module.Mod.Path), slog.String("dependentVersion", modFile.Module.Mod.Version), slog.String("dependency", dependency.Mod.Path), slog.String("dependencyVersion", dependency.Mod.Version), slog.Any("error", err))
					return err
				}

				logger.Debug("creating IS_DEPENDED_ON_BY relationship", slog.String("dependent", modFile.Module.Mod.Path), slog.String("dependentVersion", modFile.Module.Mod.Version), slog.String("dependency", dependency.Mod.Path), slog.String("dependencyVersion", dependency.Mod.Version))
				if _, err := neo4j.ExecuteQuery(ctx, driver, "MATCH (a:Module {name: $dependent, version: $dependentVersion}),(b:Module {name: $dependency, version: $dependencyVersion}) MERGE (b)-[r:IS_DEPENDED_ON_BY]->(a);", map[string]any{
					"dependent":         modFile.Module.Mod.Path,
					"dependentVersion":  modFile.Module.Mod.Version,
					"dependency":        dependency.Mod.Path,
					"dependencyVersion": dependency.Mod.Version,
				}, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("")); err != nil {
					logger.Error("failed to create IS_DEPENDED_ON_BY relationship", slog.String("dependent", modFile.Module.Mod.Path), slog.String("dependentVersion", modFile.Module.Mod.Version), slog.String("dependency", dependency.Mod.Path), slog.String("dependencyVersion", dependency.Mod.Version), slog.Any("error", err))
					return err
				}
			}

			processModules(ctx, dependsOn, knownModules, g, sem, goProxyClient, driver)

			return nil
		})
	}
}

func setupNeo4j(ctx context.Context) (neo4j.DriverWithContext, error) {
	slog.Debug("creating neo4j driver")
	driver, err := neo4j.NewDriverWithContext("neo4j://localhost", neo4j.NoAuth())
	if err != nil {
		slog.Error("failed to create neo4j driver", slog.Any("error", err))
		return nil, err
	}

	slog.Debug("verifying neo4j driver connectivity")
	if err := driver.VerifyConnectivity(ctx); err != nil {
		slog.Error("failed to verify neo4j driver connectivity", slog.Any("error", err))
		return nil, err
	}

	slog.Debug("creating neo4j session")
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: ""})
	defer session.Close(ctx)

	slog.Debug("creating neo4j indexes")
	if _, err := session.Run(ctx, "CREATE INDEX IF NOT EXISTS FOR (m:Module) ON (m.name);", nil); err != nil {
		slog.Error("failed to create index on :Module(name)", slog.Any("error", err))
		return nil, err
	}

	if _, err := session.Run(ctx, "CREATE INDEX IF NOT EXISTS FOR (m:Module) ON (m.version);", nil); err != nil {
		slog.Error("failed to create index on :Module(version)", slog.Any("error", err))
		return nil, err
	}

	return driver, nil
}

func listSeedModules(ctx context.Context, seedFile string) ([]Module, error) {
	slog.Debug("opening seed file", slog.String("file", seedFile))
	file, err := os.Open(seedFile)
	if err != nil {
		slog.Error("failed to open seed file", slog.String("file", seedFile), slog.Any("error", err))
		return nil, err
	}
	defer file.Close()

	var repositories []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		repositories = append(repositories, scanner.Text())
	}

	// The seed repositories don't necessarily have the same module name as the repository URL (eg. github.com/owner/repo can have for module name github.com/owner/repo/v2 or even gopkg.in/repo)
	// We first need to get the module name from the go.mod file
	modules := make([]Module, 0, len(repositories))
	var mxModules sync.Mutex

	g, gCtx := errgroup.WithContext(ctx)
	sem := make(chan struct{}, parallel)

	for _, repoURL := range repositories {
		g.Go(func() error {
			sem <- struct{}{}
			defer func() {
				<-sem
			}()

			repoName := strings.TrimSuffix(repoURL[strings.LastIndex(repoURL, "/")+1:], ".git")
			repoURLHash := fmt.Sprintf("%x", sha256.Sum256([]byte(repoURL)))

			logger := slog.With(slog.String("repository", repoURL))
			clonePath := "/tmp/" + repoName + "-" + repoURLHash
			logger.Debug("cloning repository", slog.String("path", clonePath))
			if err := backoff.Retry(func() error {
				_, err := git.PlainCloneContext(gCtx, clonePath, false, &git.CloneOptions{
					URL:          repoURL,
					Depth:        1,
					SingleBranch: true,
				})
				if err != nil {
					if errors.Is(err, git.ErrRepositoryAlreadyExists) {
						logger.Debug("repository already exists, removing it now", slog.String("path", clonePath))
						if err := os.RemoveAll(clonePath); err != nil {
							logger.Error("failed to remove repository", slog.String("path", clonePath), slog.Any("error", err))
							return err
						}
					} else {
						logger.Error("failed to clone repository", slog.String("path", clonePath), slog.Any("error", err))
					}

					return err
				}

				return nil
			}, backoff.WithContext(backoff.NewExponentialBackOff(), gCtx)); err != nil {
				logger.Error("failed to clone repository", slog.String("path", clonePath), slog.Any("error", err))
				return err
			}
			defer func() {
				logger.Debug("removing repository", slog.String("path", clonePath))
				if err := os.RemoveAll(clonePath); err != nil {
					logger.Error("failed to remove repository", slog.String("path", clonePath), slog.Any("error", err))
				}
			}()

			if err := filepath.WalkDir(clonePath, func(path string, info os.DirEntry, _ error) error {
				if info.Type().IsDir() {
					return nil
				}

				if info.Name() != "go.mod" {
					return nil
				}

				logger.Debug("parsing go.mod file", slog.String("path", path))

				file, err := os.Open(path)
				if err != nil {
					logger.Error("failed to open go.mod file", slog.String("path", path), slog.Any("error", err))
					return err
				}
				defer file.Close()

				data, err := io.ReadAll(file)
				if err != nil {
					logger.Error("failed to read go.mod file", slog.String("path", path), slog.Any("error", err))
					return err
				}

				parsedFile, err := modfile.Parse(path, data, nil)
				if err != nil {
					logger.Warn("failed to parse go.mod file", slog.String("path", path), slog.Any("error", err))
					return nil
				}
				logger.Debug("go.mod file parsed", slog.String("path", path))

				if parsedFile.Module == nil {
					logger.Warn("go.mod file does not contain module information", slog.String("path", path))
					return nil
				}

				mxModules.Lock()
				modules = append(modules, Module{
					URL:    repoURL,
					Module: parsedFile.Module.Mod.Path,
				})
				mxModules.Unlock()

				return nil
			}); err != nil {
				logger.Error("failed to walk repository", slog.String("path", clonePath), slog.Any("error", err))
				return err
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		slog.Error("failed to list modules", slog.Any("error", err))
		return nil, err
	}

	close(sem)

	return modules, nil
}
