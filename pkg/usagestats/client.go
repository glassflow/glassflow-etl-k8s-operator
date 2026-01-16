package usagestats

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/go-logr/logr"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

type Client struct {
	endpoint       string
	username       string
	password       string
	installationID string

	token       string
	tokenExpiry time.Time
	tokenMu     sync.RWMutex

	httpClient *http.Client
	log        logr.Logger
	enabled    bool
}

type AuthResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
}

type Event struct {
	InstallationID string         `json:"installation_id"`
	EventName      string         `json:"event_name"`
	EventSource    string         `json:"event_source"`
	Timestamp      string         `json:"timestamp"`
	Properties     map[string]any `json:"properties"`
}

type TrackResponse struct {
	Success    bool   `json:"success"`
	Message    string `json:"message"`
	EventCount int    `json:"event_count"`
}

const (
	retryAttempts = 3
	retryDelay    = 10 * time.Second
)

func NewClient(endpoint, username, password, installationID string, enabled bool, log logr.Logger) *Client {
	if !enabled {
		return &Client{enabled: false}
	}

	// Validate required fields - if any are missing, disable usage stats gracefully
	if endpoint == "" || username == "" || password == "" || installationID == "" {
		log.Info("Usage stats disabled: missing required configuration",
			"endpoint_empty", endpoint == "",
			"username_empty", username == "",
			"password_empty", password == "",
			"installation_id_empty", installationID == "")
		return &Client{enabled: false}
	}

	return &Client{
		endpoint:       endpoint,
		username:       username,
		password:       password,
		installationID: installationID,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		log:     log,
		enabled: true,
	}
}

func (c *Client) IsEnabled() bool {
	return c.enabled
}

func (c *Client) authenticate(ctx context.Context) error {
	log := logf.FromContext(ctx)

	url := fmt.Sprintf("%s/auth/login", c.endpoint)

	reqBody := map[string]string{
		"username": c.username,
		"password": c.password,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	log.Info("usage stats auth: sending authentication request", "url", url, "endpoint", c.endpoint)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("create auth request: %s", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("auth request failed: %s", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		err = fmt.Errorf("auth failed with status %d: %s", resp.StatusCode, string(body))
		return err
	}

	var authResp AuthResponse
	if err = json.NewDecoder(resp.Body).Decode(&authResp); err != nil {
		return fmt.Errorf("decode auth response: %s", err)
	}

	c.tokenMu.Lock()
	c.token = authResp.AccessToken
	c.tokenExpiry = time.Now().Add(time.Duration(authResp.ExpiresIn) * time.Second)
	c.tokenMu.Unlock()

	log.Info("usage stats auth: authentication successful", "token_expires_in", authResp.ExpiresIn)

	return nil
}

func (c *Client) getToken(ctx context.Context) (string, error) {
	c.tokenMu.RLock()
	token := c.token
	expiry := c.tokenExpiry
	c.tokenMu.RUnlock()

	if token != "" && time.Now().Before(expiry.Add(-30*time.Second)) {
		return token, nil
	}

	if err := c.authenticate(ctx); err != nil {
		return "", err
	}

	c.tokenMu.RLock()
	token = c.token
	c.tokenMu.RUnlock()

	return token, nil
}

func (c *Client) SendEvent(ctx context.Context, eventName, eventSource string, properties map[string]any) {
	log := logf.FromContext(ctx)
	if !c.enabled {
		return
	}

	log.Info("usage stats: sending event", "event", eventName, "source", eventSource, "properties", properties)

	go func() {
		usageStatsCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := c.sendEventSync(usageStatsCtx, eventName, eventSource, properties); err != nil {
			log.Info("usage stats event send failed", "event", eventName, "source", eventSource, "error", err)
		} else {
			log.Info("usage stats: event sent successfully", "event", eventName, "source", eventSource)
		}
	}()
}

func (c *Client) sendEventSync(ctx context.Context, eventName, eventSource string, properties map[string]any) error {
	log := logf.FromContext(ctx)
	if properties == nil {
		properties = make(map[string]any)
	}

	event := Event{
		InstallationID: c.installationID,
		EventName:      eventName,
		EventSource:    eventSource,
		Timestamp:      time.Now().UTC().Format(time.RFC3339),
		Properties:     properties,
	}

	jsonData, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %s", err)
	}

	url := fmt.Sprintf("%s/track", c.endpoint)

	log.Info("usage stats: sending event request")

	err = retry.Do(func() error {
		token, err := c.getToken(ctx)
		if err != nil {
			return fmt.Errorf("get token: %s", err)
		}

		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
		if err != nil {
			return fmt.Errorf("create request: %s", err)
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("accept", "application/json")
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("request failed: %s", err)
		}

		defer resp.Body.Close() //nolint:errcheck
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("usage stats failed with status %d: %s", resp.StatusCode, string(body))
		}

		var trackResp TrackResponse
		if err := json.NewDecoder(resp.Body).Decode(&trackResp); err != nil {
			return fmt.Errorf("decode response: %s", err)
		}

		log.Info("usage stats: event sent successfully", "event", eventName, "response", trackResp, "status", resp.StatusCode)

		return nil
	},
		retry.OnRetry(func(n uint, err error) {
			log.Info("usage stats: retrying event send", "event", eventName, "attempt", n+1, "error", err)
		}),
		retry.Attempts(retryAttempts),
		retry.Delay(retryDelay),
		retry.DelayType(retry.BackOffDelay),
		retry.LastErrorOnly(true),
	)

	return err
}

func HashPipelineID(pipelineID string) string {
	hash := md5.Sum([]byte(pipelineID))
	return fmt.Sprintf("%x", hash)
}
