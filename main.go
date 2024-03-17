package main

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

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

func main() {
	ctx := context.Background()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	logger.Debug("creating neo4j driver")
	driver, err := neo4j.NewDriverWithContext("neo4j://localhost", neo4j.NoAuth())
	if err != nil {
		logger.Error("failed to create neo4j driver", slog.Any("error", err))
		os.Exit(1)
	}
	defer driver.Close(ctx)

	logger.Debug("verifying neo4j driver connectivity")
	if err := driver.VerifyConnectivity(ctx); err != nil {
		logger.Error("failed to verify neo4j driver connectivity", slog.Any("error", err))
		os.Exit(1)
	}

	logger.Debug("creating neo4j session")
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: ""})
	defer session.Close(ctx)

	logger.Debug("creating neo4j indexes")
	if _, err := session.Run(ctx, "CREATE INDEX IF NOT EXISTS FOR (m:Module) ON (m.name);", nil); err != nil {
		logger.Error("failed to create index on :Module(name)", slog.Any("error", err))
		os.Exit(1)
	}

	if _, err := session.Run(ctx, "CREATE INDEX IF NOT EXISTS FOR (m:Module) ON (m.version);", nil); err != nil {
		logger.Error("failed to create index on :Module(version)", slog.Any("error", err))
		os.Exit(1)
	}

	logger.Debug("opening seed file", slog.String("file", seedFile))
	file, err := os.Open(seedFile)
	if err != nil {
		logger.Error("failed to open seed file", slog.String("file", seedFile), slog.Any("error", err))
		os.Exit(1)
	}
	defer file.Close()

	g, gCtx := errgroup.WithContext(ctx)
	sem := make(chan struct{}, parallel)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		repoURL := scanner.Text()
		repoName := repoURL[strings.LastIndex(repoURL, "/")+1:]
		repoName = strings.TrimSuffix(repoName, ".git")

		logger := logger.With(slog.String("repository", repoURL))

		sem <- struct{}{}
		g.Go(func() error {
			defer func() {
				<-sem
			}()

			clonePath := "/tmp/" + repoName
			logger.Debug("cloning repository", slog.String("path", clonePath))
			if err := backoff.Retry(func() error {
				_, err := git.PlainCloneContext(gCtx, clonePath, false, &git.CloneOptions{
					URL:          repoURL,
					Depth:        1,
					SingleBranch: true,
				})
				if err != nil {
					if !errors.Is(err, git.ErrRepositoryAlreadyExists) {
						logger.Error("failed to clone repository", slog.String("path", clonePath), slog.Any("error", err))
						return err
					}

					logger.Warn("repository already exists", slog.String("path", clonePath))

					r, err := git.PlainOpen(clonePath)
					if err != nil {
						logger.Error("failed to open repository", slog.String("path", clonePath), slog.Any("error", err))
						return err
					}

					w, err := r.Worktree()
					if err != nil {
						logger.Error("failed to get worktree", slog.String("path", clonePath), slog.Any("error", err))
						return err
					}

					logger.Debug("pulling repository", slog.String("path", clonePath))
					if err := w.PullContext(gCtx, &git.PullOptions{
						RemoteName:   "origin",
						Depth:        1,
						SingleBranch: true,
					}); err != nil {
						logger.Error("failed to pull repository", slog.String("path", clonePath), slog.Any("error", err))
						return err
					}
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

				logger.Debug("creating module node", slog.String("name", parsedFile.Module.Mod.Path), slog.String("version", parsedFile.Module.Mod.Version))
				if _, err := neo4j.ExecuteQuery(ctx, driver, "MERGE (m:Module {name: $name, version: $version}) RETURN m", map[string]any{
					"name":    parsedFile.Module.Mod.Path,
					"version": parsedFile.Module.Mod.Version,
				}, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("")); err != nil {
					logger.Error("failed to create module node", slog.String("name", parsedFile.Module.Mod.Path), slog.Any("error", err))
					return err
				}

				logger.Debug("processing direct dependencies")
				for _, dependency := range parsedFile.Require {
					if dependency.Indirect {
						continue
					}

					logger.Debug("creating dependency module node", slog.String("name", dependency.Mod.Path), slog.String("version", dependency.Mod.Version))
					if _, err := neo4j.ExecuteQuery(ctx, driver, "MERGE (m:Module {name: $name, version: $version}) RETURN m", map[string]any{
						"name":    dependency.Mod.Path,
						"version": dependency.Mod.Version,
					}, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("")); err != nil {
						logger.Error("failed to create dependency module node", slog.String("name", dependency.Mod.Path), slog.Any("error", err))
						return err
					}

					logger.Debug("creating DEPENDS_ON relationship", slog.String("dependent", parsedFile.Module.Mod.Path), slog.String("dependentVersion", parsedFile.Module.Mod.Version), slog.String("dependency", dependency.Mod.Path), slog.String("dependencyVersion", dependency.Mod.Version))
					if _, err := neo4j.ExecuteQuery(ctx, driver, "MATCH (a:Module {name: $dependent, version: $dependentVersion}),(b:Module {name: $dependency, version: $dependencyVersion}) MERGE (a)-[r:DEPENDS_ON]->(b);", map[string]any{
						"dependent":         parsedFile.Module.Mod.Path,
						"dependentVersion":  parsedFile.Module.Mod.Version,
						"dependency":        dependency.Mod.Path,
						"dependencyVersion": dependency.Mod.Version,
					}, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("")); err != nil {
						logger.Error("failed to create DEPENDS_ON relationship", slog.String("dependent", parsedFile.Module.Mod.Path), slog.String("dependentVersion", parsedFile.Module.Mod.Version), slog.String("dependency", dependency.Mod.Path), slog.String("dependencyVersion", dependency.Mod.Version), slog.Any("error", err))
						return err
					}

					logger.Debug("creating IS_DEPENDED_ON_BY relationship", slog.String("dependent", parsedFile.Module.Mod.Path), slog.String("dependentVersion", parsedFile.Module.Mod.Version), slog.String("dependency", dependency.Mod.Path), slog.String("dependencyVersion", dependency.Mod.Version))
					if _, err := neo4j.ExecuteQuery(ctx, driver, "MATCH (a:Module {name: $dependent, version: $dependentVersion}),(b:Module {name: $dependency, version: $dependencyVersion}) MERGE (b)-[r:IS_DEPENDED_ON_BY]->(a);", map[string]any{
						"dependent":         parsedFile.Module.Mod.Path,
						"dependentVersion":  parsedFile.Module.Mod.Version,
						"dependency":        dependency.Mod.Path,
						"dependencyVersion": dependency.Mod.Version,
					}, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("")); err != nil {
						logger.Error("failed to create IS_DEPENDED_ON_BY relationship", slog.String("dependent", parsedFile.Module.Mod.Path), slog.String("dependentVersion", parsedFile.Module.Mod.Version), slog.String("dependency", dependency.Mod.Path), slog.String("dependencyVersion", dependency.Mod.Version), slog.Any("error", err))
						return err
					}
				}

				return nil
			}); err != nil {
				logger.Error("failed to walk directory", slog.Any("error", err))
				return err
			}

			return nil
		})
	}

	if err := scanner.Err(); err != nil {
		logger.Error("failed to read seed file", slog.String("file", seedFile), slog.Any("error", err))
		os.Exit(1)
	}

	if err := g.Wait(); err != nil {
		logger.Error("failed to process repositories", slog.Any("error", err))
		os.Exit(1)
	}

	logger.Debug("success")
}
