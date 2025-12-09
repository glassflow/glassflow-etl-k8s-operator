package tracking

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

	"go.uber.org/zap"
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
	log        *zap.Logger
	enabled    bool
}

type AuthResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
}

type Event struct {
	InstallationID string                 `json:"installation_id"`
	EventName      string                 `json:"event_name"`
	EventSource    string                 `json:"event_source"`
	Timestamp      string                 `json:"timestamp"`
	Properties     map[string]interface{} `json:"properties"`
}

type TrackResponse struct {
	Success    bool   `json:"success"`
	Message    string `json:"message"`
	EventCount int    `json:"event_count"`
}

func NewClient(endpoint, username, password, installationID string, enabled bool, log *zap.Logger) *Client {
	if !enabled {
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
	url := fmt.Sprintf("%s/auth/login", c.endpoint)

	reqBody := map[string]string{
		"username": c.username,
		"password": c.password,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		if c.log != nil {
			c.log.Debug("tracking auth: failed to marshal request", zap.Error(err))
		}
		return fmt.Errorf("marshal auth request: %w", err)
	}

	if c.log != nil {
		c.log.Debug("tracking auth: sending authentication request",
			zap.String("url", url),
			zap.String("endpoint", c.endpoint))
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		if c.log != nil {
			c.log.Debug("tracking auth: failed to create request", zap.Error(err))
		}
		return fmt.Errorf("create auth request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		if c.log != nil {
			c.log.Debug("tracking auth: request failed", zap.Error(err), zap.String("url", url))
		}
		return fmt.Errorf("auth request failed: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil && c.log != nil {
			c.log.Debug("failed to close response body", zap.Error(closeErr))
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		err := fmt.Errorf("auth failed with status %d: %s", resp.StatusCode, string(body))
		if c.log != nil {
			c.log.Debug("tracking auth: authentication failed",
				zap.Int("status", resp.StatusCode),
				zap.String("response", string(body)),
				zap.Error(err))
		}
		return err
	}

	var authResp AuthResponse
	if err := json.NewDecoder(resp.Body).Decode(&authResp); err != nil {
		if c.log != nil {
			c.log.Debug("tracking auth: failed to decode response", zap.Error(err))
		}
		return fmt.Errorf("decode auth response: %w", err)
	}

	c.tokenMu.Lock()
	c.token = authResp.AccessToken
	c.tokenExpiry = time.Now().Add(time.Duration(authResp.ExpiresIn) * time.Second)
	c.tokenMu.Unlock()

	if c.log != nil {
		c.log.Debug("tracking auth: authentication successful", zap.Int("token_expires_in", authResp.ExpiresIn))
	}

	return nil
}

func (c *Client) getToken(ctx context.Context) (string, error) {
	c.tokenMu.RLock()
	token := c.token
	expiry := c.tokenExpiry
	c.tokenMu.RUnlock()

	if token != "" && time.Now().Before(expiry.Add(-30*time.Second)) {
		if c.log != nil {
			c.log.Debug("tracking: using cached token", zap.Time("expires_at", expiry))
		}
		return token, nil
	}

	if c.log != nil {
		c.log.Debug("tracking: token expired or missing, authenticating")
	}

	if err := c.authenticate(ctx); err != nil {
		if c.log != nil {
			c.log.Debug("tracking: authentication failed", zap.Error(err))
		}
		return "", err
	}

	c.tokenMu.RLock()
	token = c.token
	c.tokenMu.RUnlock()

	return token, nil
}

func (c *Client) SendEvent(ctx context.Context, eventName, eventSource string, properties map[string]interface{}) {
	if !c.enabled {
		return
	}

	if c.log != nil {
		c.log.Debug("tracking: sending event",
			zap.String("event", eventName),
			zap.String("source", eventSource),
			zap.Any("properties", properties))
	}

	go func() {
		if err := c.sendEventSync(ctx, eventName, eventSource, properties); err != nil {
			if c.log != nil {
				c.log.Debug("tracking event send failed",
					zap.String("event", eventName),
					zap.String("source", eventSource),
					zap.Error(err))
			}
		} else if c.log != nil {
			c.log.Debug("tracking: event sent successfully",
				zap.String("event", eventName),
				zap.String("source", eventSource))
		}
	}()
}

func (c *Client) sendEventSync(
	ctx context.Context,
	eventName, eventSource string,
	properties map[string]interface{},
) error {
	token, err := c.getToken(ctx)
	if err != nil {
		if c.log != nil {
			c.log.Debug("tracking: failed to get token", zap.String("event", eventName), zap.Error(err))
		}
		return fmt.Errorf("get token: %w", err)
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
		if c.log != nil {
			c.log.Debug("tracking: failed to marshal event", zap.String("event", eventName), zap.Error(err))
		}
		return fmt.Errorf("marshal event: %w", err)
	}

	url := fmt.Sprintf("%s/track", c.endpoint)

	if c.log != nil {
		c.log.Debug("tracking: sending event request",
			zap.String("url", url),
			zap.String("event", eventName),
			zap.String("installation_id", c.installationID),
			zap.Int("payload_size", len(jsonData)))
	}

	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if attempt > 1 && c.log != nil {
			c.log.Debug("tracking: retrying event send",
				zap.String("event", eventName),
				zap.Int("attempt", attempt),
				zap.Int("max_retries", maxRetries))
		}

		resp, err := c.makeTrackRequest(ctx, url, jsonData, token, eventName)
		if err != nil {
			if attempt < maxRetries {
				if c.log != nil {
					c.log.Debug("tracking: request failed, will retry",
						zap.String("event", eventName),
						zap.Int("attempt", attempt),
						zap.Error(err),
						zap.Int("retry_after", attempt))
				}
				time.Sleep(time.Duration(attempt) * time.Second)
				continue
			}
			if c.log != nil {
				c.log.Debug("tracking: request failed after max retries",
					zap.String("event", eventName),
					zap.Int("attempt", attempt),
					zap.Error(err))
			}
			return fmt.Errorf("request failed: %w", err)
		}

		if resp.StatusCode == http.StatusUnauthorized {
			token, resp, err = c.handleUnauthorized(ctx, url, jsonData, eventName, attempt)
			if err != nil {
				if attempt < maxRetries {
					time.Sleep(time.Duration(attempt) * time.Second)
					continue
				}
				return err
			}
		}

		if resp.StatusCode != http.StatusOK {
			shouldRetry := c.handleNonOKStatus(resp, eventName, attempt, maxRetries)
			if shouldRetry {
				time.Sleep(time.Duration(attempt) * time.Second)
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			return fmt.Errorf("tracking failed with status %d: %s", resp.StatusCode, string(body))
		}

		return c.handleSuccessResponse(resp, eventName)
	}

	if c.log != nil {
		c.log.Debug("tracking: max retries exceeded", zap.String("event", eventName), zap.Int("max_retries", maxRetries))
	}
	return fmt.Errorf("max retries exceeded")
}

func (c *Client) makeTrackRequest(
	ctx context.Context,
	url string,
	jsonData []byte,
	token string,
	eventName string,
) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		if c.log != nil {
			c.log.Debug("tracking: failed to create request", zap.String("event", eventName), zap.Error(err))
		}
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("accept", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	return resp, nil
}

func (c *Client) handleUnauthorized(
	ctx context.Context,
	url string,
	jsonData []byte,
	eventName string,
	attempt int,
) (string, *http.Response, error) {
	if c.log != nil {
		c.log.Debug("tracking: received unauthorized, re-authenticating",
			zap.String("event", eventName),
			zap.Int("attempt", attempt))
	}

	if err := c.authenticate(ctx); err != nil {
		if c.log != nil {
			c.log.Debug("tracking: re-authentication failed", zap.String("event", eventName), zap.Error(err))
		}
		return "", nil, fmt.Errorf("re-authenticate: %w", err)
	}

	token, err := c.getToken(ctx)
	if err != nil {
		if c.log != nil {
			c.log.Debug("tracking: failed to get new token after re-auth", zap.String("event", eventName), zap.Error(err))
		}
		return "", nil, fmt.Errorf("get new token: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", nil, fmt.Errorf("create retry request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("accept", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", nil, fmt.Errorf("retry request failed: %w", err)
	}

	return token, resp, nil
}

func (c *Client) handleNonOKStatus(resp *http.Response, eventName string, attempt, maxRetries int) bool {
	body, _ := io.ReadAll(resp.Body)
	if closeErr := resp.Body.Close(); closeErr != nil && c.log != nil {
		c.log.Debug("failed to close response body", zap.Error(closeErr))
	}

	if attempt < maxRetries {
		if c.log != nil {
			c.log.Debug("tracking: non-OK status, will retry",
				zap.String("event", eventName),
				zap.Int("status", resp.StatusCode),
				zap.String("response", string(body)),
				zap.Int("attempt", attempt))
		}
		return true
	}

	if c.log != nil {
		c.log.Debug("tracking: failed after max retries",
			zap.String("event", eventName),
			zap.Int("status", resp.StatusCode),
			zap.String("response", string(body)),
			zap.Int("attempt", attempt))
	}
	return false
}

func (c *Client) handleSuccessResponse(resp *http.Response, eventName string) error {
	var trackResp TrackResponse
	if err := json.NewDecoder(resp.Body).Decode(&trackResp); err != nil {
		if closeErr := resp.Body.Close(); closeErr != nil && c.log != nil {
			c.log.Debug("failed to close response body", zap.Error(closeErr))
		}
		if c.log != nil {
			c.log.Debug("tracking: failed to decode response", zap.String("event", eventName), zap.Error(err))
		}
		return fmt.Errorf("decode response: %w", err)
	}

	if closeErr := resp.Body.Close(); closeErr != nil && c.log != nil {
		c.log.Debug("failed to close response body", zap.Error(closeErr))
	}

	if c.log != nil {
		c.log.Debug("tracking: event sent successfully",
			zap.String("event", eventName),
			zap.Any("response", trackResp),
			zap.Int("status", resp.StatusCode))
	}

	return nil
}

func HashPipelineID(pipelineID string) string {
	hash := md5.Sum([]byte(pipelineID))
	return fmt.Sprintf("%x", hash)
}
