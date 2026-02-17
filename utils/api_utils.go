package utils

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// CachedAPIResult holds a cached API response
type CachedAPIResult struct {
	Data      map[string]interface{}
	Timestamp time.Time
}

var (
	apiCache   = make(map[string]*CachedAPIResult)
	apiCacheMu sync.RWMutex
	apiCacheTTL = 3600 * time.Second
)

// PerformCheck performs an API check with caching
func PerformCheck(apiKey string, baseURL string, searchType string, query string) (map[string]interface{}, error) {
	// Build cache key
	cacheKey := fmt.Sprintf("%s:%s:%s:%s", apiKey, baseURL, searchType, query)

	// Check cache
	apiCacheMu.RLock()
	if cached, ok := apiCache[cacheKey]; ok {
		if time.Since(cached.Timestamp) < apiCacheTTL {
			apiCacheMu.RUnlock()
			return cached.Data, nil
		}
	}
	apiCacheMu.RUnlock()

	// Build URL
	encodedQuery := url.PathEscape(query)
	reqURL := fmt.Sprintf("%s%s?type=%s", baseURL, encodedQuery, searchType)

	// Make request
	client := &http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-API-Key", apiKey)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == 200 {
		var result map[string]interface{}
		if err := json.Unmarshal(body, &result); err != nil {
			return nil, err
		}

		// Cache result
		apiCacheMu.Lock()
		apiCache[cacheKey] = &CachedAPIResult{
			Data:      result,
			Timestamp: time.Now(),
		}
		apiCacheMu.Unlock()

		return result, nil
	}

	return map[string]interface{}{
		"success": false,
		"error":   fmt.Sprintf("API error: %d - %s", resp.StatusCode, string(body)),
	}, nil
}