package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
)

// CodeChangeHandler decides whether to restart or hot-reload
type CodeChangeHandler struct {
	restartCallback func()
	reloadCallback  func(string)
	lastReload      time.Time
	cooldown        time.Duration
	coreFiles       map[string]bool
}

func NewCodeChangeHandler(restart func(), reload func(string)) *CodeChangeHandler {
	return &CodeChangeHandler{
		restartCallback: restart,
		reloadCallback:  reload,
		cooldown:        2 * time.Second,
		coreFiles: map[string]bool{
			"main.go":                          true,
			"run.go":                           true,
			"token_api.go":                     true,
			filepath.Join("utils", "config.go"): true,
		},
	}
}

func (h *CodeChangeHandler) shouldIgnore(path string) bool {
	return strings.HasSuffix(path, "config.json") ||
		strings.Contains(path, "__pycache__") ||
		strings.Contains(path, ".git") ||
		strings.HasSuffix(path, ".pyc")
}

func (h *CodeChangeHandler) shouldReload(path string) bool {
	if h.shouldIgnore(path) {
		return false
	}
	return strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "database.json")
}

func (h *CodeChangeHandler) needsFullRestart(path string) bool {
	norm := filepath.ToSlash(filepath.Clean(path))
	for core := range h.coreFiles {
		if strings.Contains(norm, filepath.ToSlash(core)) {
			return true
		}
	}
	return false
}

func (h *CodeChangeHandler) tryReload(path string) {
	now := time.Now()
	if now.Sub(h.lastReload) <= h.cooldown {
		return
	}
	h.lastReload = now
	log.Printf("[watcher] Detected change in %s", path)

	if h.needsFullRestart(path) {
		log.Println("[watcher] Core file changed, performing full restart")
		h.restartCallback()
	} else {
		log.Printf("[watcher] Performing hot reload for: %s", path)
		h.reloadCallback(path)
	}
}

// BotSupervisor manages the bot process
type BotSupervisor struct {
	mu        sync.Mutex
	process   *exec.Cmd
	shouldRun bool
	watcher   *fsnotify.Watcher
}

func NewBotSupervisor() *BotSupervisor {
	return &BotSupervisor{
		shouldRun: true,
	}
}

func (s *BotSupervisor) StartBot() {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Println("[supervisor] Starting bot process...")

	// Build first
	build := exec.Command("go", "build", "-o", "lostselfbot", "main.go")
	build.Stdout = os.Stdout
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		log.Printf("[supervisor] Build failed: %v", err)
		return
	}

	s.process = exec.Command("./lostselfbot")
	s.process.Stdout = os.Stdout
	s.process.Stderr = os.Stderr
	s.process.Env = append(os.Environ(), "HOT_RELOAD_ENABLED=1")

	if err := s.process.Start(); err != nil {
		log.Printf("[supervisor] Error starting bot: %v", err)
		s.shouldRun = false
		return
	}

	// Monitor in background
	go func() {
		if err := s.process.Wait(); err != nil {
			log.Printf("[supervisor] Bot process exited: %v", err)
		}
	}()
}

func (s *BotSupervisor) RestartBot() {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Println("[supervisor] Restarting bot...")

	if s.process != nil && s.process.Process != nil {
		s.process.Process.Signal(syscall.SIGTERM)
		done := make(chan error, 1)
		go func() { done <- s.process.Wait() }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			log.Println("[supervisor] Bot didn't terminate gracefully, forcing kill")
			s.process.Process.Kill()
		}
	}

	s.mu.Unlock()
	s.StartBot()
	s.mu.Lock()
}

func (s *BotSupervisor) HotReload(filePath string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.process == nil || s.process.ProcessState != nil {
		log.Println("[supervisor] Cannot hot reload: Bot process not running")
		return false
	}

	filePath = filepath.ToSlash(filePath)

	var reloadMsg string
	if strings.Contains(filePath, "/cogs/") {
		cogName := strings.TrimSuffix(filepath.Base(filePath), ".go")
		reloadMsg = fmt.Sprintf("HOT_RELOAD:COG:%s\n", cogName)
		log.Printf("[supervisor] Hot reloading cog: %s", cogName)
	} else if strings.Contains(filePath, "/utils/") {
		baseName := strings.TrimSuffix(filepath.Base(filePath), ".go")
		var modulePath string
		if strings.Contains(filePath, "utils/database/") {
			modulePath = "utils.database." + baseName
		} else {
			modulePath = "utils." + baseName
		}
		reloadMsg = fmt.Sprintf("HOT_RELOAD:MODULE:%s\n", modulePath)
		log.Printf("[supervisor] Hot reloading module: %s", modulePath)
	} else {
		reloadMsg = fmt.Sprintf("HOT_RELOAD:FILE:%s\n", filePath)
		log.Printf("[supervisor] Hot reloading file: %s", filePath)
	}

	if err := os.WriteFile(".hot_reload_signal", []byte(reloadMsg), 0644); err != nil {
		log.Printf("[supervisor] Error writing hot reload signal: %v", err)
		return false
	}
	return true
}

func (s *BotSupervisor) SetupFileWatcher() error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("error creating watcher: %w", err)
	}
	s.watcher = watcher

	handler := NewCodeChangeHandler(
		func() { s.RestartBot() },
		func(path string) { s.HotReload(path) },
	)

	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&(fsnotify.Write|fsnotify.Create) != 0 {
					if handler.shouldReload(event.Name) {
						handler.tryReload(event.Name)
					}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Printf("[watcher] Error: %v", err)
			}
		}
	}()

	paths := []string{".", "./cogs", "./utils", "./config"}
	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			if err := watcher.Add(p); err != nil {
				log.Printf("[watcher] Error watching %s: %v", p, err)
			} else {
				log.Printf("[watcher] Watching directory: %s", p)
			}
		}
	}

	return nil
}

func (s *BotSupervisor) Run() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("[supervisor] Starting bot supervisor...")

	if err := s.SetupFileWatcher(); err != nil {
		log.Fatalf("[supervisor] Failed to setup file watcher: %v", err)
	}

	s.StartBot()

	go func() {
		<-sigChan
		log.Println("[supervisor] Shutdown signal received")
		s.shouldRun = false
		s.Cleanup()
		os.Exit(0)
	}()

	for s.shouldRun {
		time.Sleep(1 * time.Second)
	}

	s.Cleanup()
}

func (s *BotSupervisor) Cleanup() {
	log.Println("[supervisor] Cleaning up...")

	if s.watcher != nil {
		s.watcher.Close()
	}

	s.mu.Lock()
	if s.process != nil && s.process.Process != nil {
		s.process.Process.Signal(syscall.SIGTERM)
		done := make(chan error, 1)
		go func() { done <- s.process.Wait() }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			s.process.Process.Kill()
		}
	}
	s.mu.Unlock()

	os.Remove(".hot_reload_signal")
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	supervisor := NewBotSupervisor()
	supervisor.Run()
}