package kontur

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Authenticator interface {
	ConfigureRequest(*http.Request) error
}

type Client struct {
	baseURL string
	http    *http.Client
	auth    Authenticator
}

func NewClient(baseURL string, auth Authenticator) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http: &http.Client{
			Timeout: 90 * time.Second,
		},
		auth: auth,
	}
}

func (c *Client) DoJSON(ctx context.Context, method, path string, body any, target any) (*http.Response, error) {
	req, err := c.newRequest(ctx, method, path, body)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if target != nil {
		if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func (c *Client) DoText(ctx context.Context, method, path string, body any) (string, *http.Response, error) {
	req, err := c.newRequest(ctx, method, path, body)
	if err != nil {
		return "", nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()

	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", nil, err
	}
	return string(payload), resp, nil
}

func (c *Client) Download(ctx context.Context, path, target string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.AbsoluteURL(path), nil)
	if err != nil {
		return err
	}
	if c.auth != nil {
		if err := c.auth.ConfigureRequest(req); err != nil {
			return err
		}
	}

	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	file, err := os.Create(target)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, resp.Body)
	return err
}

func (c *Client) AbsoluteURL(path string) string {
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		return path
	}
	if c.baseURL == "" {
		return path
	}
	base, err := url.Parse(c.baseURL)
	if err != nil {
		return path
	}
	ref, err := url.Parse(path)
	if err != nil {
		return path
	}
	return base.ResolveReference(ref).String()
}

func (c *Client) newRequest(ctx context.Context, method, path string, body any) (*http.Request, error) {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(payload)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.AbsoluteURL(path), reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json, text/plain, */*")
	if body != nil {
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
	}
	if c.auth != nil {
		if err := c.auth.ConfigureRequest(req); err != nil {
			return nil, err
		}
	}
	return req, nil
}

func (c *Client) do(req *http.Request) (*http.Response, error) {
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%s %s failed: %d %s", req.Method, req.URL.String(), resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return resp, nil
}
