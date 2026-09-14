package checker

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"
	"time"

	"github.com/UllasSG/Uptime-status-checker/internal/config"
)

type JobResult struct {
	Target     config.Target
	Up         bool
	Err        string
	StatusCode int
	Latency    time.Duration
	CheckedAt  time.Time
}

type Checker struct {
	Client *http.Client
	//notifier
	//result quueue
}

func NewChecker(c *http.Client) *Checker {
	return &Checker{
		Client: c,
	}
}
func (c *Checker) Check(ctx context.Context, target config.Target) (*JobResult, error) {
	ctxT, cancel := context.WithTimeout(
		ctx,
		time.Duration(target.TimeoutSecs)*time.Second,
	)
	defer cancel()

	req, err := http.NewRequestWithContext(ctxT, http.MethodGet, target.URL, nil)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	resp, err := c.Client.Do(req)
	if err != nil {
		return &JobResult{
			Target:    target,
			Up:        false,
			Err:       err.Error(),
			Latency:   time.Since(start),
			CheckedAt: time.Now(),
		}, nil
	}
	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	up := slices.Contains(target.ExpectedStatus, resp.StatusCode)

	result := JobResult{
		Target:     target,
		Up:         up,
		Err:        "",
		StatusCode: resp.StatusCode,
		Latency:    time.Since(start),
		CheckedAt:  time.Now(),
	}

	if !up {
		result.Err = fmt.Sprintf("unexpected status code: %d", resp.StatusCode)
	}

	return &result, nil
}
