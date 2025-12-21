package cmd

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"

	"github.com/Thiht/go-command"
	"github.com/Thiht/go-stats/goproxy"
	"github.com/Thiht/go-stats/semver"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
	"golang.org/x/sync/errgroup"
)

func ProcessModulesHandler(driver neo4j.DriverWithContext, goProxyClient goproxy.Client) command.Handler {
	return func(ctx context.Context, flagSet *flag.FlagSet, _ []string) int {
		parallel := command.Lookup[int](flagSet, "parallel")
		seedFile := command.Lookup[string](flagSet, "seed-file")
		offset := command.Lookup[int64](flagSet, "offset")

		modules, err := loadModules(ctx, seedFile, offset)
		if err != nil {
			slog.ErrorContext(ctx, "failed to load initial modules", slog.Any("error", err))
			return 1
		}

		progress := progressbar.Default(int64(len(modules)))

		partitions := partitionModules(modules, parallel)
		for i, p := range partitions {
			slog.DebugContext(ctx, "partition created", slog.Int("partition", i), slog.Int("count", len(p)))
		}

		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(parallel)

		for i, p := range partitions {
			g.Go(func() error {
				for _, m := range p {
					slog.DebugContext(gCtx, "processing module", slog.Int("partition", i), slog.Any("module", m))

					if err := progress.Add(1); err != nil {
						slog.WarnContext(gCtx, "failed to update progress bar", slog.Int("partition", i), slog.Any("error", err))
					}

					if err := processModule(gCtx, m, goProxyClient, driver); err != nil {
						slog.ErrorContext(gCtx, "failed to process module", slog.Int("partition", i), slog.Any("module", m), slog.Any("error", err))
						return err
					}

					slog.DebugContext(gCtx, "module processed", slog.Int("partition", i), slog.Any("module", m))
				}

				return nil
			})
		}

		if err := g.Wait(); err != nil {
			slog.ErrorContext(ctx, "failed to process repositories", slog.Any("error", err))
			os.Exit(1)
		}

		return 0
	}
}

func loadModules(ctx context.Context, seedFile string, offset int64) ([]module.Version, error) {
	slog.DebugContext(ctx, "opening seed file", slog.String("file", seedFile))
	seedFileHandler, err := os.Open(seedFile)
	if err != nil {
		slog.ErrorContext(ctx, "failed to open seed file", slog.String("file", seedFile), slog.Any("error", err))
		return nil, fmt.Errorf("failed to open seed file: %w", err)
	}
	defer seedFileHandler.Close()

	slog.DebugContext(ctx, "estimating seed file line count", slog.String("file", seedFile))
	file, err := os.Stat(seedFile)
	if err != nil {
		slog.ErrorContext(ctx, "failed to stat seed file", slog.String("file", seedFile), slog.Any("error", err))
		return nil, fmt.Errorf("failed to stat seed file: %w", err)
	}

	const averageLineLength = 80 // awk '{ total += length($0); count++ } END { print total/count }' file
	estimatedCount := file.Size() / averageLineLength

	slog.DebugContext(ctx, "reading seed file", slog.String("file", seedFile), slog.Int64("estimatedCount", estimatedCount))
	modules := make([]module.Version, 0, estimatedCount)

	reader := csv.NewReader(seedFileHandler)
	reader.FieldsPerRecord = 3

	// Read header
	if _, err := reader.Read(); err != nil && !errors.Is(err, io.EOF) {
		slog.ErrorContext(ctx, "failed to read header", slog.String("file", seedFile), slog.Any("error", err))
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	nbLines := int64(0)
	for {
		record, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			slog.ErrorContext(ctx, "failed to read seed file", slog.String("file", seedFile), slog.Any("error", err))
			return nil, fmt.Errorf("failed to read seed file: %w", err)
		}

		nbLines++
		if nbLines <= offset {
			continue
		}

		modules = append(modules, module.Version{
			Path:    record[1],
			Version: record[2],
		})
	}

	slog.DebugContext(ctx, "loaded initial modules", slog.Int("count", len(modules)), slog.Int64("estimatedCount", estimatedCount))

	return modules, nil
}

func partitionModules(modules []module.Version, nbPartitions int) [][]module.Version {
	partitions := make([][]module.Version, nbPartitions)

	for _, m := range modules {
		p := bucket(m.Path, nbPartitions)
		partitions[p] = append(partitions[p], m)
	}

	return partitions
}

func bucket(s string, n int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return int(h.Sum32() % uint32(n))
}

func processModule(ctx context.Context, m module.Version, goProxyClient goproxy.Client, driver neo4j.DriverWithContext) error {
	logger := slog.With(slog.Any("module", m))

	moduleInfo, err := getModuleInfo(ctx, m, goProxyClient)
	if err != nil {
		logger.WarnContext(ctx, "failed to get module info", slog.Any("error", err))
		return nil
	}

	semver, err := semver.Parse(m.Version)
	if err != nil {
		logger.ErrorContext(ctx, "failed to parse module version", slog.String("version", m.Version), slog.Any("error", err))
		return fmt.Errorf("failed to parse module version: %w", err)
	}

	logger.Debug("creating module node")
	if _, err := neo4j.ExecuteQuery(ctx, driver, `
		MERGE (m:Module { name: $name, version: $version })
		SET m += {
			org: $org, host: $host,
			versionTime: date($versionTime), versionMajor: $versionMajor, versionMinor: $versionMinor, versionPatch: $versionPatch, versionLabel: $versionLabel,
		}
		RETURN m
	`, map[string]any{
		"name": m.Path, "version": m.Version,
		"org": extractOrg(m.Path), "host": extractHost(m.Path),
		"versionTime": moduleInfo.Time.Format("2006-01-02"), "versionMajor": semver.Major, "versionMinor": semver.Minor, "versionPatch": semver.Patch, "versionLabel": semver.Label,
	}, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("")); err != nil {
		logger.ErrorContext(ctx, "failed to create module node", slog.Any("error", err))
		return fmt.Errorf("failed to create module node: %w", err)
	}

	logger.DebugContext(ctx, "processing direct dependencies")
	modFile, err := getModFile(ctx, m, goProxyClient)
	if err != nil {
		logger.WarnContext(ctx, "failed to get module go.mod file", slog.Any("error", err))
		return nil
	}

	dependencies := make([]map[string]any, 0, len(modFile.Require))
	for _, dependency := range modFile.Require {
		if dependency.Indirect {
			continue
		}

		dependencies = append(dependencies, map[string]any{
			"dependencyName":    dependency.Mod.Path,
			"dependencyVersion": dependency.Mod.Version,
		})
	}

	logger.DebugContext(ctx, "creating module nodes and relationships for dependencies", slog.Int("dependenciesCount", len(dependencies)))

	if _, err := neo4j.ExecuteQuery(ctx, driver, `
		UNWIND $dependencies AS dep
		MERGE (dependency:Module { name: dep.dependencyName, version: dep.dependencyVersion })
		WITH dependency
		MATCH (dependent:Module { name: $dependentName, version: $dependentVersion })
		CREATE (dependent)-[:DEPENDS_ON]->(dependency)
		RETURN dependency, dependent
	`, map[string]any{
		"dependentName": m.Path, "dependentVersion": m.Version,
		"dependencies": dependencies,
	}, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase(""), neo4j.ExecuteQueryWithTransactionConfig(neo4j.WithTxTimeout(30*time.Second))); err != nil {
		logger.ErrorContext(ctx, "failed to create dependencies nodes and relationships", slog.Int("dependenciesCount", len(dependencies)), slog.Any("error", err))
		return fmt.Errorf("failed to create dependencies nodes and relationships: %w", err)
	}

	return nil
}

func getModuleInfo(ctx context.Context, m module.Version, goProxyClient goproxy.Client) (goproxy.ModuleInfo, error) {
	logger := slog.With(slog.Any("module", m))

	logger.DebugContext(ctx, "getting module info from goproxy (cached)")
	moduleInfo, err := goProxyClient.GetModuleInfo(ctx, m.Path, m.Version, true)
	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return goproxy.ModuleInfo{}, fmt.Errorf("timeout while getting module info: %w", err)
		}

		if !errors.Is(err, goproxy.ErrModuleNotFound) {
			return goproxy.ModuleInfo{}, fmt.Errorf("failed to get module info: %w", err)
		}

		logger.DebugContext(ctx, "getting module info from goproxy")
		moduleInfo, err = goProxyClient.GetModuleInfo(ctx, m.Path, m.Version, false)
		if err != nil {
			if errors.As(err, &netErr) && netErr.Timeout() {
				return goproxy.ModuleInfo{}, fmt.Errorf("timeout while getting module info: %w", err)
			}

			if errors.Is(err, goproxy.ErrModuleNotFound) {
				return goproxy.ModuleInfo{}, fmt.Errorf("module info not found: %w", err)
			}

			return goproxy.ModuleInfo{}, fmt.Errorf("failed to get module info: %w", err)
		}
	}

	return moduleInfo, nil
}

func getModFile(ctx context.Context, m module.Version, goProxyClient goproxy.Client) (*modfile.File, error) {
	logger := slog.With(slog.Any("module", m))

	logger.DebugContext(ctx, "getting mod file from goproxy (cached)")
	modFile, err := goProxyClient.GetModuleModFile(ctx, m.Path, m.Version, true)
	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return nil, fmt.Errorf("timeout while getting module go.mod file: %w", err)
		}

		if errors.Is(err, goproxy.ErrInvalidModFile) {
			return nil, fmt.Errorf("invalid go.mod file: %w", err)
		}

		if !errors.Is(err, goproxy.ErrModuleNotFound) {
			return nil, fmt.Errorf("failed to get go.mod file: %w", err)
		}

		logger.DebugContext(ctx, "getting mod file from goproxy")
		modFile, err = goProxyClient.GetModuleModFile(ctx, m.Path, m.Version, false)
		if err != nil {
			if errors.As(err, &netErr) && netErr.Timeout() {
				return nil, fmt.Errorf("timeout while getting module go.mod file: %w", err)
			}

			if errors.Is(err, goproxy.ErrInvalidModFile) {
				return nil, fmt.Errorf("invalid go.mod file: %w", err)
			}

			if errors.Is(err, goproxy.ErrModuleNotFound) {
				// This means the module doesn't have a go.mod file
				return nil, fmt.Errorf("module go.mod file not found: %w", err)
			}

			return nil, fmt.Errorf("failed to get module go.mod file: %w", err)
		}
	}

	return modFile, nil
}

func extractHost(modulePath string) string {
	return strings.Split(modulePath, "/")[0]
}

func extractOrg(modulePath string) string {
	switch {
	case strings.HasPrefix(modulePath, "golang.org/"),
		strings.HasPrefix(modulePath, "github.com/pkg/"):
		return "golang"

	case strings.HasPrefix(modulePath, "github.com/"):
		org := strings.Split(modulePath, "/")[1]
		return org

	case strings.HasPrefix(modulePath, "google.golang.org/"):
		return "google"

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

	case strings.HasPrefix(modulePath, "go.etcd.io/"):
		return "etcd-io"

	default:
		return ""
	}
}
