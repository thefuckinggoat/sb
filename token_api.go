package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const apiPort = 24683

// Rate limiter
type rateLimiter struct {
	mu       sync.Mutex
	attempts map[string]*attemptInfo
}

type attemptInfo struct {
	count     int
	timestamp int64
}

var limiter = &rateLimiter{
	attempts: make(map[string]*attemptInfo),
}

const (
	maxValidationAttempts = 10
	rateLimitWindow       = 60
)

func (rl *rateLimiter) checkLimit(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now().Unix()

	// Clean old entries
	for k, v := range rl.attempts {
		if now-v.timestamp > rateLimitWindow {
			delete(rl.attempts, k)
		}
	}

	info, exists := rl.attempts[ip]
	if !exists {
		rl.attempts[ip] = &attemptInfo{count: 1, timestamp: now}
		return false
	}

	if info.count >= maxValidationAttempts {
		if now-info.timestamp < rateLimitWindow {
			return true // rate limited
		}
		rl.attempts[ip] = &attemptInfo{count: 1, timestamp: now}
		return false
	}

	info.count++
	info.timestamp = now
	return false
}

// CORS middleware
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin == "" {
			origin = "*"
		}
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With, X-Captcha-Key, X-Captcha-Rqtoken")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Max-Age", "86400")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// Handlers

func indexHandler(w http.ResponseWriter, r *http.Request) {
	templatePath := filepath.Join("templates", "index.html")
	data, err := os.ReadFile(templatePath)
	if err != nil {
		http.Error(w, fmt.Sprintf("Template not found: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html")
	w.Write(data)
}

func validateTokenHandler(w http.ResponseWriter, r *http.Request) {
	clientIP := r.RemoteAddr
	if idx := strings.LastIndex(clientIP, ":"); idx != -1 {
		clientIP = clientIP[:idx]
	}

	if limiter.checkLimit(clientIP) {
		writeJSON(w, http.StatusTooManyRequests, map[string]string{
			"status":  "error",
			"message": "Too many validation requests. Please try again later.",
		})
		return
	}

	var body struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Token == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"status":  "error",
			"message": "Token is required.",
		})
		return
	}

	log.Printf("[api] Validating token from IP: %s", clientIP)

	userInfo, err := getUserInfo(body.Token)
	if err != nil || userInfo == nil {
		log.Println("[api] Token validation failed - invalid or expired token")
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"status":  "error",
			"message": "Invalid or expired token.",
		})
		return
	}

	log.Printf("[api] Token validation successful for user: %s", userInfo["username"])
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status": "success",
		"token":  body.Token,
		"user":   userInfo,
	})
}

func directLoginHandler(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err == nil && body.Token != "" {
		validateTokenHandler(w, r)
		return
	}

	writeJSON(w, http.StatusBadRequest, map[string]string{
		"status":  "error",
		"message": "Direct login is no longer supported. Please use the popup authentication method.",
	})
}

func staticFileHandler(w http.ResponseWriter, r *http.Request) {
	filename := strings.TrimPrefix(r.URL.Path, "/static/")
	filePath := filepath.Join("static", filename)

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	var contentType string
	switch {
	case strings.HasSuffix(filename, ".css"):
		contentType = "text/css"
	case strings.HasSuffix(filename, ".js"):
		contentType = "application/javascript"
	case strings.HasSuffix(filename, ".png"):
		contentType = "image/png"
	case strings.HasSuffix(filename, ".jpg"), strings.HasSuffix(filename, ".jpeg"):
		contentType = "image/jpeg"
	default:
		contentType = "application/octet-stream"
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Write(data)
}

func getUserInfo(token string) (map[string]interface{}, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequest("GET", "https://discord.com/api/v9/users/@me", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", token)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("token verification failed: status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var userInfo map[string]interface{}
	if err := json.Unmarshal(body, &userInfo); err != nil {
		return nil, err
	}

	log.Printf("[api] Successfully obtained token for user: %s", userInfo["username"])
	return userInfo, nil
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	os.MkdirAll("templates", 0755)

	mux := http.NewServeMux()
	mux.HandleFunc("/", indexHandler)
	mux.HandleFunc("/static/", staticFileHandler)
	mux.HandleFunc("/validate-token", validateTokenHandler)
	mux.HandleFunc("/direct-login", directLoginHandler)

	handler := corsMiddleware(mux)

	addr := fmt.Sprintf("0.0.0.0:%d", apiPort)
	log.Printf("[api] Discord Token API server started at http://%s", addr)

	if err := http.ListenAndServe(addr, handler); err != nil {
		log.Fatalf("[api] Server error: %v", err)
	}
}