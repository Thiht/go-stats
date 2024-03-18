package goproxy

import (
	"context"
	"encoding/json"
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
	Version string
	Time    time.Time
	Origin  struct {
		VCS  string
		URL  string
		Hash string
	}
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

var ErrModuleNotFound = fmt.Errorf("module not found")

func (c *client) GetModuleLatestInfo(ctx context.Context, modulePath string) (ModuleInfo, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.apiURL+"/"+modulePath+"/@latest", nil)
	if err != nil {
		return ModuleInfo{}, err
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return ModuleInfo{}, err
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
		return ModuleInfo{}, err
	}

	return info, nil
}

func (c *client) GetModuleInfo(ctx context.Context, modulePath, version string) (ModuleInfo, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.apiURL+"/"+modulePath+"/@v/"+version+".info", nil)
	if err != nil {
		return ModuleInfo{}, err
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return ModuleInfo{}, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return ModuleInfo{}, fmt.Errorf("unexpected status code: %d", response.StatusCode)
	}

	var info ModuleInfo
	if err := json.NewDecoder(response.Body).Decode(&info); err != nil {
		return ModuleInfo{}, err
	}

	return info, nil
}

func (c *client) GetModuleModFile(ctx context.Context, modulePath, version string) (*modfile.File, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.apiURL+"/"+modulePath+"/@v/"+version+".mod", nil)
	if err != nil {
		return nil, err
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, err
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
		return nil, err
	}

	file, err := modfile.Parse("go.mod", data, nil)
	if err != nil {
		return nil, err
	}

	return file, nil
}
