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
	"time"

	"github.com/Thiht/go-command"
	"github.com/Thiht/go-stats/goproxy"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
	"golang.org/x/sync/errgroup"
)

// FIXME: remove globals
var (
	moduleInfoCache       *lru.Cache[module.Version, goproxy.ModuleInfo]
	latestModuleInfoCache *lru.Cache[string, goproxy.ModuleInfo]
)

func init() {
	var err error
	moduleInfoCache, err = lru.New[module.Version, goproxy.ModuleInfo](1024)
	if err != nil {
		panic(fmt.Sprintf("failed to create module info cache: %v", err))
	}

	latestModuleInfoCache, err = lru.New[string, goproxy.ModuleInfo](1024)
	if err != nil {
		panic(fmt.Sprintf("failed to create latest module info cache: %v", err))
	}
}

func ProcessModulesHandler(driver neo4j.DriverWithContext, goProxyClient goproxy.Client) command.Handler {
	return func(ctx context.Context, flagSet *flag.FlagSet, _ []string) int {
		parallel := command.Lookup[int](flagSet, "parallel")
		seedFile := command.Lookup[string](flagSet, "seed-file")
		offset := command.Lookup[int64](flagSet, "offset")

		initialModules, err := loadInitialModules(seedFile, offset)
		if err != nil {
			slog.Error("failed to load initial modules", slog.Any("error", err))
			return 1
		}

		var (
			nbModules = int64(len(initialModules))
			progress  = progressbar.Default(nbModules)
		)

		modulesToProcess := make(chan module.Version, 1_000)

		go func() {
			defer func() {
				slog.Debug("closing processing queue")
				close(modulesToProcess)
			}()

			for _, m := range initialModules {
				slog.Debug("adding module to processing queue", slog.Any("module", m))
				modulesToProcess <- m
			}
		}()

		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(parallel)

		for m := range modulesToProcess {
			g.Go(func() error {
				defer func() {
					if err := progress.Add(1); err != nil {
						slog.Warn("failed to update progress bar", slog.Any("error", err))
					}
				}()

				slog.Debug("processing module", slog.Any("module", m))

				if err := processModule(gCtx, m, goProxyClient, driver); err != nil {
					slog.Error("failed to process module", slog.Any("module", m), slog.Any("error", err))
					return err
				}

				slog.Debug("module processed", slog.Any("module", m))

				return nil
			})
		}

		if err := g.Wait(); err != nil {
			slog.Error("failed to process repositories", slog.Any("error", err))
			os.Exit(1)
		}

		return 0
	}
}

func loadInitialModules(seedFile string, offset int64) ([]module.Version, error) {
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
	nbLines := int64(0)
	for scanner.Scan() {
		line := scanner.Text()

		nbLines++
		if nbLines <= offset {
			continue
		}

		modulePath, moduleVersion, _ := strings.Cut(line, " ")
		modules = append(modules, module.Version{
			Path:    modulePath,
			Version: moduleVersion,
		})
	}
	if err := scanner.Err(); err != nil {
		slog.Error("failed to read seed file", slog.String("file", seedFile), slog.Any("error", err))
		return nil, fmt.Errorf("failed to read seed file: %w", err)
	}

	slog.Debug("loaded initial modules", slog.Int("count", len(modules)), slog.Int64("estimatedCount", estimatedCount))

	return modules, nil
}

func processModule(ctx context.Context, m module.Version, goProxyClient goproxy.Client, driver neo4j.DriverWithContext) error {
	logger := slog.With(slog.Any("module", m))

	moduleInfo, err := getModuleInfo(ctx, m, goProxyClient)
	if err != nil {
		logger.Warn("failed to get module info", slog.Any("error", err))
		return nil
	}

	latestModuleInfo, err := getLatestModuleInfo(ctx, m, goProxyClient)
	if err != nil {
		logger.Warn("failed to get latest module info", slog.Any("error", err))
		return nil
	}

	semver, err := parseVersion(m.Version)
	if err != nil {
		logger.Error("failed to parse module version", slog.String("version", m.Version), slog.Any("error", err))
		return fmt.Errorf("failed to parse module version: %w", err)
	}

	latestSemver, err := parseVersion(latestModuleInfo.Version)
	if err != nil {
		logger.Error("failed to parse latest module version", slog.String("version", latestModuleInfo.Version), slog.Any("error", err))
		return fmt.Errorf("failed to parse latest module version: %w", err)
	}

	logger.Debug("creating module node")
	if _, err := neo4j.ExecuteQuery(ctx, driver, `
		MERGE (m:Module { name: $name, version: $version })
		SET m += {
			org: $org, host: $host, latest: $latest,
			versionTime: date($versionTime), versionMajor: $versionMajor, versionMinor: $versionMinor, versionPatch: $versionPatch, versionLabel: $versionLabel,
			latestTime: date($latestTime), latestMajor: $latestMajor, latestMinor: $latestMinor, latestPatch: $latestPatch, latestLabel: $latestLabel
		}
		RETURN m
	`, map[string]any{
		"name": m.Path, "version": m.Version,
		"org": extractOrg(m.Path), "host": extractHost(m.Path), "latest": latestModuleInfo.Version,
		"versionTime": moduleInfo.Time.Format("2006-01-02"), "versionMajor": semver.Major, "versionMinor": semver.Minor, "versionPatch": semver.Patch, "versionLabel": semver.Label,
		"latestTime": latestModuleInfo.Time.Format("2006-01-02"), "latestMajor": latestSemver.Major, "latestMinor": latestSemver.Minor, "latestPatch": latestSemver.Patch, "latestLabel": latestSemver.Label,
	}, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("")); err != nil {
		logger.Error("failed to create module node", slog.Any("error", err))
		return fmt.Errorf("failed to create module node: %w", err)
	}

	logger.Debug("processing direct dependencies")

	modFile, err := getModFile(ctx, m, goProxyClient)
	if err != nil {
		logger.Warn("failed to get module go.mod file", slog.Any("error", err))
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

	logger.Debug("creating module nodes and relationships for dependencies", slog.Int("dependenciesCount", len(dependencies)))

	if _, err := neo4j.ExecuteQuery(ctx, driver, `
		UNWIND $dependencies AS dep
		MERGE (dependency:Module { name: dep.dependencyName, version: dep.dependencyVersion })
		MERGE (dependent:Module {name: $dependentName, version: $dependentVersion })
		MERGE (dependent)-[:DEPENDS_ON]->(dependency)
		RETURN dependency, dependent
	`, map[string]any{
		"dependentName": m.Path, "dependentVersion": m.Version,
		"dependencies": dependencies,
	}, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase(""), neo4j.ExecuteQueryWithTransactionConfig(neo4j.WithTxTimeout(30*time.Second))); err != nil {
		logger.Error("failed to create dependencies nodes and relationships", slog.Int("dependenciesCount", len(dependencies)), slog.Any("error", err))
		return fmt.Errorf("failed to create dependencies nodes and relationships: %w", err)
	}

	return nil
}

func getModuleInfo(ctx context.Context, m module.Version, goProxyClient goproxy.Client) (goproxy.ModuleInfo, error) {
	logger := slog.With(slog.Any("module", m))

	logger.Debug("getting module info from cache")
	if latestModuleInfo, ok := moduleInfoCache.Get(m); ok {
		return latestModuleInfo, nil
	}

	logger.Debug("getting module info from goproxy (cached)")
	moduleInfo, err := goProxyClient.GetModuleInfo(ctx, m.Path, m.Version, true)
	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return goproxy.ModuleInfo{}, fmt.Errorf("timeout while getting module info: %w", err)
		}

		if !errors.Is(err, goproxy.ErrModuleNotFound) {
			return goproxy.ModuleInfo{}, fmt.Errorf("failed to get module info: %w", err)
		}

		logger.Debug("getting module info from goproxy")
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

	_ = moduleInfoCache.Add(m, moduleInfo)
	logger.Debug("module info cached")

	return moduleInfo, nil
}

func getLatestModuleInfo(ctx context.Context, m module.Version, goProxyClient goproxy.Client) (goproxy.ModuleInfo, error) {
	logger := slog.With(slog.Any("module", m))

	logger.Debug("getting latest module info from cache")
	if latestModuleInfo, ok := latestModuleInfoCache.Get(m.Path); ok {
		return latestModuleInfo, nil
	}

	logger.Debug("getting latest module info from goproxy (cached)")
	latestModuleInfo, err := goProxyClient.GetModuleLatestInfo(ctx, m.Path, true)
	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return goproxy.ModuleInfo{}, fmt.Errorf("timeout while getting latest module info: %w", err)
		}

		if !errors.Is(err, goproxy.ErrModuleNotFound) {
			return goproxy.ModuleInfo{}, fmt.Errorf("failed to get latest module info: %w", err)
		}

		logger.Debug("getting latest module info from goproxy")
		latestModuleInfo, err = goProxyClient.GetModuleLatestInfo(ctx, m.Path, false)
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

	_ = latestModuleInfoCache.Add(m.Path, latestModuleInfo)
	logger.Debug("latest module info cached")

	return latestModuleInfo, nil
}

func getModFile(ctx context.Context, m module.Version, goProxyClient goproxy.Client) (*modfile.File, error) {
	logger := slog.With(slog.Any("module", m))

	logger.Debug("getting mod file from goproxy (cached)")
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

		logger.Debug("getting mod file from goproxy ")
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

type semanticVersion struct {
	Major string
	Minor string
	Patch string
	Label string
}

func parseVersion(version string) (semver semanticVersion, err error) {
	version = strings.TrimPrefix(version, "v")

	version, label, ok := strings.Cut(version, "-")
	if !ok {
		version, label, _ = strings.Cut(version, "+")
	}

	tokens := strings.Split(version, ".")
	if len(tokens) != 3 {
		return semanticVersion{}, fmt.Errorf("invalid version format: %s", version)
	}

	return semanticVersion{Major: tokens[0], Minor: tokens[1], Patch: tokens[2], Label: label}, nil
}
