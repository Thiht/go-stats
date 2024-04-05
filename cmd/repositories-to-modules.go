package cmd

import (
	"bufio"
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/Thiht/go-command"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-git/go-git/v5"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
	"golang.org/x/sync/errgroup"
)

var parallel = runtime.NumCPU()

func RepositoriesToModulesHandler() command.Handler {
	return func(ctx context.Context, flagSet *flag.FlagSet, _ []string) int {
		inputFile := command.Lookup[string](flagSet, "input-file")
		outputFile := command.Lookup[string](flagSet, "output-file")

		slog.Debug("opening input file", slog.String("file", inputFile))
		inputFileHandler, err := os.Open(inputFile)
		if err != nil {
			slog.Error("failed to open input file", slog.String("file", inputFile), slog.Any("error", err))
			return 1
		}
		defer inputFileHandler.Close()

		slog.Debug("reading input file", slog.String("file", inputFile))
		var repositories []string
		scanner := bufio.NewScanner(inputFileHandler)
		for scanner.Scan() {
			repository, err := normalizeRepository(scanner.Text())
			if err != nil {
				continue
			}

			repositories = append(repositories, repository)
		}
		if err := scanner.Err(); err != nil {
			slog.Error("failed to read input file", slog.String("file", inputFile), slog.Any("error", err))
			return 1
		}

		// The seed repositories don't necessarily have the same module name as the repository URL (eg. github.com/owner/repo can have for module name github.com/owner/repo/v2 or even gopkg.in/repo)
		// We first need to get the module name from the go.mod file
		modules := make([]module.Version, 0, len(repositories))
		var mxModules sync.Mutex

		g, gCtx := errgroup.WithContext(ctx)
		sem := make(chan struct{}, parallel)

		progress := progressbar.Default(int64(len(repositories)))
		for _, repoURL := range repositories {
			sem <- struct{}{}
			time.Sleep(100 * time.Millisecond)
			g.Go(func() error {
				defer func() {
					_ = progress.Add(1)
					<-sem
				}()

				ctx := gCtx

				repoName := repoURL[strings.LastIndex(repoURL, "/")+1:]
				repoURL += ".git"
				repoURLHash := fmt.Sprintf("%x", sha256.Sum256([]byte(repoURL)))

				logger := slog.With(slog.String("repository", repoURL))

				clonePath := "/tmp/" + repoName + "-" + repoURLHash
				logger.Debug("cloning repository", slog.String("path", clonePath))
				if err := backoff.Retry(func() error {
					_, err := git.PlainCloneContext(ctx, clonePath, false, &git.CloneOptions{
						URL:          repoURL,
						Depth:        1,
						SingleBranch: true,
					})
					if err != nil {
						switch {
						case errors.Is(err, git.ErrRepositoryAlreadyExists):
							logger.Debug("repository already exists, removing it now", slog.String("path", clonePath))
							if err := os.RemoveAll(clonePath); err != nil {
								logger.Error("failed to remove repository", slog.String("path", clonePath), slog.Any("error", err))
								return fmt.Errorf("failed to remove repository: %w", err)
							}

						default:
							logger.Error("failed to clone repository", slog.String("path", clonePath), slog.Any("error", err))
						}

						return fmt.Errorf("failed to clone repository: %w", err)
					}

					return nil
				}, backoff.WithContext(backoff.NewExponentialBackOff(), ctx)); err != nil {
					logger.Error("failed to clone repository", slog.String("path", clonePath), slog.Any("error", err))
					return fmt.Errorf("failed to clone repository after multiple attempts: %w", err)
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
						return fmt.Errorf("failed to open go.mod file: %w", err)
					}
					defer file.Close()

					data, err := io.ReadAll(file)
					if err != nil {
						logger.Error("failed to read go.mod file", slog.String("path", path), slog.Any("error", err))
						return fmt.Errorf("failed to read go.mod file: %w", err)
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
					modules = append(modules, parsedFile.Module.Mod)
					mxModules.Unlock()

					return nil
				}); err != nil {
					logger.Error("failed to walk repository", slog.String("path", clonePath), slog.Any("error", err))
					return fmt.Errorf("failed to walk repository: %w", err)
				}

				return nil
			})
		}

		if err := g.Wait(); err != nil {
			slog.Error("failed to list some modules", slog.Any("error", err))
		}

		close(sem)

		slog.Debug("opening output file", slog.String("file", outputFile))
		outputFileHandler, err := os.Create(outputFile)
		if err != nil {
			slog.Error("failed to open output file", slog.String("file", outputFile), slog.Any("error", err))
			return 1
		}
		defer outputFileHandler.Close()

		slog.Debug("writing output file", slog.String("file", outputFile))
		for _, module := range modules {
			if _, err := fmt.Fprintf(outputFileHandler, "%s\n", module.Path); err != nil {
				slog.Error("failed to write module", slog.String("module", module.Path), slog.Any("error", err))
				return 1
			}
		}

		return 0
	}
}

var reGitHubRepository = regexp.MustCompile(`^https://github.com/[^/]+/[^/]+$`)

func normalizeRepository(repository string) (string, error) {
	if strings.HasPrefix(repository, "http://github.com/") {
		repository = strings.TrimPrefix(repository, "http://")
		repository = "https://" + repository
	}

	repository = strings.TrimSuffix(repository, ".git")
	repository = strings.TrimSuffix(repository, "/")

	if !reGitHubRepository.MatchString(repository) {
		return "", fmt.Errorf("unhandled repository URL: %s", repository)
	}

	return repository, nil
}
