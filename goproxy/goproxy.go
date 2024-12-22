package goproxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"golang.org/x/mod/modfile"
)

const (
	proxyURL = "https://proxy.golang.org"
	indexURL = "https://index.golang.org"
)

type ModuleInfo struct {
	Version string    `json:"Version"`
	Time    time.Time `json:"Time"`
	Origin  struct {
		VCS  string `json:"VCS"`
		URL  string `json:"URL"`
		Hash string `json:"Hash"`
	} `json:"Origin"`
}

type Index struct {
	Path      string    `json:"Path"`
	Version   string    `json:"Version"`
	Timestamp time.Time `json:"Timestamp"`
}

type client struct {
	httpClient *http.Client
}

type Client interface {
	ListIndex(ctx context.Context, since time.Time) ([]Index, error)
	GetModuleLatestInfo(ctx context.Context, modulePath string, cachedOnly bool) (ModuleInfo, error)
	GetModuleInfo(ctx context.Context, modulePath, version string, cachedOnly bool) (ModuleInfo, error)
	GetModuleModFile(ctx context.Context, modulePath, version string, cachedOnly bool) (*modfile.File, error)
}

func NewGoProxyClient() Client {
	return &client{
		httpClient: &http.Client{
			Timeout: 3 * time.Second,
		},
	}
}

var (
	ErrModuleNotFound = errors.New("module not found")
	ErrInvalidModFile = errors.New("invalid mod file")
)

const ListIndexMaxLimit = 2000

func (c *client) ListIndex(ctx context.Context, since time.Time) ([]Index, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, indexURL+"/index", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	queryParams := request.URL.Query()
	queryParams.Add("since", since.Format(time.RFC3339Nano))
	queryParams.Add("limit", strconv.Itoa(ListIndexMaxLimit))
	queryParams.Add("include", "")
	request.URL.RawQuery = queryParams.Encode()

	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", response.StatusCode)
	}

	indexes := make([]Index, 0, ListIndexMaxLimit)

	decoder := json.NewDecoder(response.Body)
	for {
		var index Index
		if err := decoder.Decode(&index); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, fmt.Errorf("failed to decode response: %w", err)
		}

		indexes = append(indexes, index)
	}

	return indexes, nil
}

func (c *client) GetModuleLatestInfo(ctx context.Context, modulePath string, cachedOnly bool) (ModuleInfo, error) {
	cachedOnlyPath := ""
	if cachedOnly {
		cachedOnlyPath = "/cached-only"
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, proxyURL+cachedOnlyPath+"/"+modulePath+"/@latest", nil)
	if err != nil {
		return ModuleInfo{}, fmt.Errorf("failed to create request: %w", err)
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return ModuleInfo{}, fmt.Errorf("failed to execute request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		if response.StatusCode == http.StatusNotFound {
			return ModuleInfo{}, ErrModuleNotFound
		}

		return ModuleInfo{}, fmt.Errorf("unexpected status code: %d", response.StatusCode)
	}

	var info ModuleInfo
	if err := json.NewDecoder(response.Body).Decode(&info); err != nil {
		return ModuleInfo{}, fmt.Errorf("failed to decode response: %w", err)
	}

	return info, nil
}

func (c *client) GetModuleInfo(ctx context.Context, modulePath, version string, cachedOnly bool) (ModuleInfo, error) {
	cachedOnlyPath := ""
	if cachedOnly {
		cachedOnlyPath = "/cached-only"
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, proxyURL+cachedOnlyPath+"/"+modulePath+"/@v/"+version+".info", nil)
	if err != nil {
		return ModuleInfo{}, fmt.Errorf("failed to create request: %w", err)
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return ModuleInfo{}, fmt.Errorf("failed to execute request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return ModuleInfo{}, fmt.Errorf("unexpected status code: %d", response.StatusCode)
	}

	var info ModuleInfo
	if err := json.NewDecoder(response.Body).Decode(&info); err != nil {
		return ModuleInfo{}, fmt.Errorf("failed to decode response: %w", err)
	}

	return info, nil
}

func (c *client) GetModuleModFile(ctx context.Context, modulePath, version string, cachedOnly bool) (*modfile.File, error) {
	cachedOnlyPath := ""
	if cachedOnly {
		cachedOnlyPath = "/cached-only"
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, proxyURL+cachedOnlyPath+"/"+modulePath+"/@v/"+version+".mod", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		if response.StatusCode == http.StatusNotFound {
			return nil, ErrModuleNotFound
		}

		return nil, fmt.Errorf("unexpected status code: %d", response.StatusCode)
	}

	data, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	file, err := modfile.Parse("go.mod", data, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to parse response as modfile: %w", ErrInvalidModFile)
	}

	return file, nil
}
