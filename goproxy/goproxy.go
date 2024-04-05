package goproxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"golang.org/x/mod/modfile"
)

const (
	apiURL = "https://proxy.golang.org"
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

type client struct {
	apiURL     string
	httpClient *http.Client
}

type Client interface {
	GetModuleLatestInfo(ctx context.Context, modulePath string) (ModuleInfo, error)
	GetModuleInfo(ctx context.Context, modulePath, version string) (ModuleInfo, error)
	GetModuleModFile(ctx context.Context, modulePath, version string) (*modfile.File, error)
}

func NewGoProxyClient() Client {
	return &client{
		apiURL:     apiURL,
		httpClient: &http.Client{},
	}
}

var ErrModuleNotFound = errors.New("module not found")

func (c *client) GetModuleLatestInfo(ctx context.Context, modulePath string) (ModuleInfo, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.apiURL+"/"+modulePath+"/@latest", nil)
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

func (c *client) GetModuleInfo(ctx context.Context, modulePath, version string) (ModuleInfo, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.apiURL+"/"+modulePath+"/@v/"+version+".info", nil)
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

func (c *client) GetModuleModFile(ctx context.Context, modulePath, version string) (*modfile.File, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.apiURL+"/"+modulePath+"/@v/"+version+".mod", nil)
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
		return nil, fmt.Errorf("failed to parse response as modfile: %w", err)
	}

	return file, nil
}
