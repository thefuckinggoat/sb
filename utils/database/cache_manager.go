package database

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
)

// CacheEntry holds cached data with metadata
type CacheEntry struct {
	Data                  interface{}
	CreatedAt             float64
	AccessCount           int
	LastAccessed          float64
	TTL                   float64
	CacheKey              string
	Collection            string
	InvalidationPatterns  map[string]struct{}
}

// CacheConfig defines TTL and priority for a cache type
type CacheConfig struct {
	TTL      float64
	Priority string
}

// IntelligentCache is a high-performance caching layer with LRU eviction,
// smart invalidation, and automatic TTL management
type IntelligentCache struct {
	mu                   sync.RWMutex
	maxSize              int
	defaultTTL           float64
	cache                map[string]*CacheEntry
	cacheOrder           []string // LRU ordering
	invalidationPatterns map[string]map[string]struct{}
	collectionKeys       map[string]map[string]struct{}
	cacheConfigs         map[string]CacheConfig
	stats                CacheStats
	running              bool
	stopChan             chan struct{}
}

// CacheStats tracks cache performance
type CacheStats struct {
	Hits                    int64
	Misses                  int64
	Evictions               int64
	Invalidations           int64
	MemoryPressureEvictions int64
}

func NewIntelligentCache(maxSize int, defaultTTL int) *IntelligentCache {
	if maxSize == 0 {
		maxSize = 10000
	}
	if defaultTTL == 0 {
		defaultTTL = 300
	}

	return &IntelligentCache{
		maxSize:              maxSize,
		defaultTTL:           float64(defaultTTL),
		cache:                make(map[string]*CacheEntry),
		cacheOrder:           make([]string, 0),
		invalidationPatterns: make(map[string]map[string]struct{}),
		collectionKeys:       make(map[string]map[string]struct{}),
		stopChan:             make(chan struct{}),
		cacheConfigs: map[string]CacheConfig{
			"user_lookup":    {TTL: 600, Priority: "high"},
			"user_history":   {TTL: 1800, Priority: "medium"},
			"message_search": {TTL: 300, Priority: "low"},
			"guild_stats":    {TTL: 900, Priority: "medium"},
			"presence_data":  {TTL: 120, Priority: "low"},
			"system_config":  {TTL: 3600, Priority: "high"},
		},
		stats: CacheStats{},
	}
}

func (ic *IntelligentCache) Start() {
	if ic.running {
		return
	}
	ic.running = true
	go ic.cleanupLoop()
	log.Println("[Cache] Started intelligent cache manager")
}

func (ic *IntelligentCache) Stop() {
	if !ic.running {
		return
	}
	ic.running = false
	select {
	case <-ic.stopChan:
	default:
		close(ic.stopChan)
	}
	log.Println("[Cache] Stopped cache manager")
}

func (ic *IntelligentCache) createCacheKey(collection string, operation string, query map[string]interface{},
	projection map[string]interface{}, sort []SortField, limit int, skip int) string {

	keyData := map[string]interface{}{
		"collection": collection,
		"operation":  operation,
		"query":      query,
		"projection": projection,
		"sort":       sort,
		"limit":      limit,
		"skip":       skip,
	}

	keyBytes, err := json.Marshal(keyData)
	if err != nil {
		return fmt.Sprintf("%s_%s_%v", collection, operation, query)
	}

	hash := sha256.Sum256(keyBytes)
	return fmt.Sprintf("%x", hash[:8])
}

func (ic *IntelligentCache) determineCacheType(collection string, query map[string]interface{}) string {
	if collection == "users" {
		if _, ok := query["user_id"]; ok {
			return "user_lookup"
		}
		for field := range query {
			if len(field) > 8 && field[len(field)-8:] == "_history" {
				return "user_history"
			}
		}
		return "user_lookup"
	}

	if collection == "deleted_messages" || collection == "edited_messages" {
		return "message_search"
	}

	if _, ok := query["guild_id"]; ok {
		return "guild_stats"
	}

	if _, ok := query["status"]; ok {
		return "presence_data"
	}

	return "user_lookup"
}

func (ic *IntelligentCache) createInvalidationPatterns(collection string, query map[string]interface{}) map[string]struct{} {
	patterns := map[string]struct{}{
		fmt.Sprintf("collection:%s", collection): {},
	}

	idFields := []string{"user_id", "guild_id", "channel_id", "message_id"}
	for _, field := range idFields {
		if val, ok := query[field]; ok {
			patterns[fmt.Sprintf("%s:%v", field, val)] = struct{}{}
		}
	}

	return patterns
}

func (ic *IntelligentCache) Get(collection string, operation string, query map[string]interface{},
	projection map[string]interface{}, sort []SortField, limit int, skip int) interface{} {

	cacheKey := ic.createCacheKey(collection, operation, query, projection, sort, limit, skip)

	ic.mu.Lock()
	defer ic.mu.Unlock()

	entry, ok := ic.cache[cacheKey]
	if !ok {
		ic.stats.Misses++
		return nil
	}

	currentTime := float64(time.Now().UnixMilli()) / 1000.0

	// Check TTL
	if currentTime-entry.CreatedAt > entry.TTL {
		ic.removeLocked(cacheKey)
		ic.stats.Misses++
		return nil
	}

	// Update access pattern
	entry.AccessCount++
	entry.LastAccessed = currentTime

	// Move to end (most recently used)
	ic.moveToEnd(cacheKey)

	ic.stats.Hits++
	return entry.Data
}

func (ic *IntelligentCache) Set(collection string, operation string, query map[string]interface{},
	result interface{}, projection map[string]interface{}, sort []SortField, limit int, skip int) {

	if result == nil {
		return
	}

	// Don't cache empty results
	switch v := result.(type) {
	case []map[string]interface{}:
		if len(v) == 0 {
			return
		}
	case map[string]interface{}:
		if len(v) == 0 {
			return
		}
	}

	cacheKey := ic.createCacheKey(collection, operation, query, projection, sort, limit, skip)
	cacheType := ic.determineCacheType(collection, query)
	config, ok := ic.cacheConfigs[cacheType]
	if !ok {
		config = CacheConfig{TTL: ic.defaultTTL, Priority: "medium"}
	}

	currentTime := float64(time.Now().UnixMilli()) / 1000.0
	invalidationPatterns := ic.createInvalidationPatterns(collection, query)

	entry := &CacheEntry{
		Data:                 result,
		CreatedAt:            currentTime,
		AccessCount:          1,
		LastAccessed:         currentTime,
		TTL:                  config.TTL,
		CacheKey:             cacheKey,
		Collection:           collection,
		InvalidationPatterns: invalidationPatterns,
	}

	ic.mu.Lock()
	defer ic.mu.Unlock()

	// Remove old entry if exists
	if _, exists := ic.cache[cacheKey]; exists {
		ic.removeLocked(cacheKey)
	}

	// Add new entry
	ic.cache[cacheKey] = entry
	ic.cacheOrder = append(ic.cacheOrder, cacheKey)

	// Track collection keys
	if _, ok := ic.collectionKeys[collection]; !ok {
		ic.collectionKeys[collection] = make(map[string]struct{})
	}
	ic.collectionKeys[collection][cacheKey] = struct{}{}

	// Track invalidation patterns
	for pattern := range invalidationPatterns {
		if _, ok := ic.invalidationPatterns[pattern]; !ok {
			ic.invalidationPatterns[pattern] = make(map[string]struct{})
		}
		ic.invalidationPatterns[pattern][cacheKey] = struct{}{}
	}

	// Evict if necessary
	ic.evictIfNeeded(config.Priority)
}

func (ic *IntelligentCache) evictIfNeeded(priority string) {
	if len(ic.cache) <= ic.maxSize {
		return
	}

	priorityScores := map[string]int{"low": 1, "medium": 2, "high": 3}
	currentPriority := priorityScores[priority]
	if currentPriority == 0 {
		currentPriority = 2
	}

	currentTime := float64(time.Now().UnixMilli()) / 1000.0

	type evictionCandidate struct {
		score float64
		key   string
	}

	var candidates []evictionCandidate

	for key, entry := range ic.cache {
		ageScore := (currentTime - entry.LastAccessed) / 3600.0
		accessScore := 1.0 / float64(entry.AccessCount+1)

		entryCacheType := ic.determineCacheType(entry.Collection, map[string]interface{}{})
		entryConfig, ok := ic.cacheConfigs[entryCacheType]
		entryPriority := 2
		if ok {
			if p, exists := priorityScores[entryConfig.Priority]; exists {
				entryPriority = p
			}
		}

		// Don't evict higher priority items for lower priority requests
		if entryPriority > currentPriority {
			continue
		}

		evictionScore := ageScore + accessScore - float64(entryPriority)*0.5
		candidates = append(candidates, evictionCandidate{score: evictionScore, key: key})
	}

	// Sort by eviction score (highest first) - simple selection
	for i := 0; i < len(candidates); i++ {
		for j := i + 1; j < len(candidates); j++ {
			if candidates[j].score > candidates[i].score {
				candidates[i], candidates[j] = candidates[j], candidates[i]
			}
		}
	}

	targetEvictions := len(ic.cache) - int(float64(ic.maxSize)*0.9)
	evicted := 0

	for _, c := range candidates {
		if evicted >= targetEvictions {
			break
		}
		ic.removeLocked(c.key)
		evicted++
	}

	ic.stats.Evictions += int64(evicted)
	if evicted > 0 {
		log.Printf("[Cache] Evicted %d entries due to size limit", evicted)
	}
}

func (ic *IntelligentCache) InvalidateByWrite(collection string, operation string,
	query map[string]interface{}, update map[string]interface{}) {

	patterns := map[string]struct{}{
		fmt.Sprintf("collection:%s", collection): {},
	}

	idFields := []string{"user_id", "guild_id", "channel_id", "message_id"}
	for _, field := range idFields {
		if val, ok := query[field]; ok {
			patterns[fmt.Sprintf("%s:%v", field, val)] = struct{}{}
		}
	}

	// Check $set in update
	if update != nil {
		if setOp, ok := update["$set"]; ok {
			if setMap, ok := setOp.(map[string]interface{}); ok {
				for _, field := range idFields {
					if val, ok := setMap[field]; ok {
						patterns[fmt.Sprintf("%s:%v", field, val)] = struct{}{}
					}
				}
			}
		}
	}

	ic.mu.Lock()
	defer ic.mu.Unlock()

	keysToRemove := make(map[string]struct{})
	for pattern := range patterns {
		if keys, ok := ic.invalidationPatterns[pattern]; ok {
			for key := range keys {
				keysToRemove[key] = struct{}{}
			}
		}
	}

	for key := range keysToRemove {
		ic.removeLocked(key)
	}

	ic.stats.Invalidations += int64(len(keysToRemove))

	if len(keysToRemove) > 0 {
		log.Printf("[Cache] Invalidated %d entries for %s.%s", len(keysToRemove), collection, operation)
	}
}

func (ic *IntelligentCache) InvalidateCollection(collection string) {
	ic.mu.Lock()
	defer ic.mu.Unlock()

	keys, ok := ic.collectionKeys[collection]
	if !ok {
		return
	}

	count := len(keys)
	for key := range keys {
		ic.removeLocked(key)
	}

	delete(ic.collectionKeys, collection)
	ic.stats.Invalidations += int64(count)

	if count > 0 {
		log.Printf("[Cache] Invalidated %d entries for collection %s", count, collection)
	}
}

// removeLocked removes an entry from all tracking structures. Must be called with lock held.
func (ic *IntelligentCache) removeLocked(key string) {
	entry, ok := ic.cache[key]
	if !ok {
		return
	}

	delete(ic.cache, key)

	// Remove from collection keys
	if colKeys, ok := ic.collectionKeys[entry.Collection]; ok {
		delete(colKeys, key)
	}

	// Remove from invalidation patterns
	for pattern := range entry.InvalidationPatterns {
		if patternKeys, ok := ic.invalidationPatterns[pattern]; ok {
			delete(patternKeys, key)
		}
	}

	// Remove from order
	for i, k := range ic.cacheOrder {
		if k == key {
			ic.cacheOrder = append(ic.cacheOrder[:i], ic.cacheOrder[i+1:]...)
			break
		}
	}
}

// moveToEnd moves a key to the end of the LRU order. Must be called with lock held.
func (ic *IntelligentCache) moveToEnd(key string) {
	for i, k := range ic.cacheOrder {
		if k == key {
			ic.cacheOrder = append(ic.cacheOrder[:i], ic.cacheOrder[i+1:]...)
			ic.cacheOrder = append(ic.cacheOrder, key)
			return
		}
	}
}

func (ic *IntelligentCache) cleanupLoop() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ic.stopChan:
			return
		case <-ticker.C:
			ic.cleanupExpired()
			ic.optimizeMemory()
		}
	}
}

func (ic *IntelligentCache) cleanupExpired() {
	currentTime := float64(time.Now().UnixMilli()) / 1000.0

	ic.mu.Lock()
	defer ic.mu.Unlock()

	var expiredKeys []string
	for key, entry := range ic.cache {
		if currentTime-entry.CreatedAt > entry.TTL {
			expiredKeys = append(expiredKeys, key)
		}
	}

	for _, key := range expiredKeys {
		ic.removeLocked(key)
	}

	if len(expiredKeys) > 0 {
		log.Printf("[Cache] Cleaned up %d expired entries", len(expiredKeys))
	}
}

func (ic *IntelligentCache) optimizeMemory() {
	ic.mu.Lock()
	defer ic.mu.Unlock()

	// Clean up empty invalidation patterns
	for pattern, keys := range ic.invalidationPatterns {
		if len(keys) == 0 {
			delete(ic.invalidationPatterns, pattern)
		}
	}

	// Clean up empty collection key sets
	for collection, keys := range ic.collectionKeys {
		if len(keys) == 0 {
			delete(ic.collectionKeys, collection)
		}
	}
}

func (ic *IntelligentCache) GetStats() map[string]interface{} {
	ic.mu.RLock()
	defer ic.mu.RUnlock()

	totalRequests := ic.stats.Hits + ic.stats.Misses
	hitRate := float64(0)
	if totalRequests > 0 {
		hitRate = float64(ic.stats.Hits) / float64(totalRequests) * 100
	}

	return map[string]interface{}{
		"cache_size":              len(ic.cache),
		"max_size":               ic.maxSize,
		"hit_rate":               hitRate,
		"total_requests":         totalRequests,
		"hits":                   ic.stats.Hits,
		"misses":                 ic.stats.Misses,
		"evictions":              ic.stats.Evictions,
		"invalidations":          ic.stats.Invalidations,
		"collections_cached":     len(ic.collectionKeys),
		"invalidation_patterns":  len(ic.invalidationPatterns),
	}
}

func (ic *IntelligentCache) ClearStats() {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	ic.stats = CacheStats{}
}

func (ic *IntelligentCache) ClearAll() {
	ic.mu.Lock()
	defer ic.mu.Unlock()

	ic.cache = make(map[string]*CacheEntry)
	ic.cacheOrder = make([]string, 0)
	ic.collectionKeys = make(map[string]map[string]struct{})
	ic.invalidationPatterns = make(map[string]map[string]struct{})

	log.Println("[Cache] Cleared all cache entries")
}

// Global cache instance
var _globalCache *IntelligentCache
var _globalCacheOnce sync.Once

func GetCache() *IntelligentCache {
	_globalCacheOnce.Do(func() {
		_globalCache = NewIntelligentCache(10000, 300)
	})
	return _globalCache
}

func StartCache() {
	GetCache().Start()
}

func StopCache() {
	GetCache().Stop()
}