package database

import (
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)

// QueryPerformanceMetric tracks a single query execution
type QueryPerformanceMetric struct {
	Collection              string
	Operation               string
	QuerySignature          string
	ExecutionTime           float64
	DocsExamined            int
	DocsReturned            int
	IndexUsed               string
	Timestamp               time.Time
	CacheHit                bool
	OptimizationSuggestions []string
}

// PerformanceMonitor provides comprehensive performance monitoring
type PerformanceMonitor struct {
	mu            sync.RWMutex
	dbManager     *DatabaseManager
	queryMetrics  []QueryPerformanceMetric
	slowQueries   []QueryPerformanceMetric
	indexUsage    map[string]map[string]int
	queryPatterns map[string]int
	collectionAccess map[string]map[string]int

	maxMetrics         int
	maxSlowQueries     int
	slowQueryThreshold float64
	efficiencyThreshold float64

	stats          PerfStats
	monitoringActive bool
	stopChan       chan struct{}
}

// PerfStats holds running statistics
type PerfStats struct {
	TotalQueries     int64
	SlowQueries      int64
	CacheHits        int64
	IndexScans       int64
	CollectionScans  int64
	AvgExecutionTime float64
	QueriesPerSecond float64
}

func NewPerformanceMonitor(dbManager *DatabaseManager) *PerformanceMonitor {
	return &PerformanceMonitor{
		dbManager:           dbManager,
		queryMetrics:        make([]QueryPerformanceMetric, 0),
		slowQueries:         make([]QueryPerformanceMetric, 0),
		indexUsage:          make(map[string]map[string]int),
		queryPatterns:       make(map[string]int),
		collectionAccess:    make(map[string]map[string]int),
		maxMetrics:          10000,
		maxSlowQueries:      1000,
		slowQueryThreshold:  100,
		efficiencyThreshold: 0.1,
		stopChan:            make(chan struct{}),
	}
}

func (pm *PerformanceMonitor) StartMonitoring() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.monitoringActive {
		return
	}
	pm.monitoringActive = true
	go pm.monitoringLoop()
	log.Println("[PerfMonitor] Started performance monitoring")
}

func (pm *PerformanceMonitor) StopMonitoring() {
	pm.mu.Lock()
	if !pm.monitoringActive {
		pm.mu.Unlock()
		return
	}
	pm.monitoringActive = false
	pm.mu.Unlock()

	select {
	case <-pm.stopChan:
	default:
		close(pm.stopChan)
	}
	log.Println("[PerfMonitor] Stopped performance monitoring")
}

// RecordQuery records a query execution for performance analysis
func (pm *PerformanceMonitor) RecordQuery(collection string, operation string,
	query map[string]interface{}, executionTime float64, resultCount int,
	cacheHit bool, explainData map[string]interface{}) {

	querySignature := pm.createQuerySignature(collection, operation, query)

	docsExamined := 0
	docsReturned := resultCount
	indexUsed := ""

	if explainData != nil {
		if execStats, ok := explainData["executionStats"].(map[string]interface{}); ok {
			if v, ok := execStats["totalDocsExamined"].(int); ok {
				docsExamined = v
			}
			if v, ok := execStats["totalDocsReturned"].(int); ok {
				docsReturned = v
			}
		}
		if qp, ok := explainData["queryPlanner"].(map[string]interface{}); ok {
			if wp, ok := qp["winningPlan"].(map[string]interface{}); ok {
				indexUsed = pm.extractIndexName(wp)
			}
		}
	}

	metric := QueryPerformanceMetric{
		Collection:     collection,
		Operation:      operation,
		QuerySignature: querySignature,
		ExecutionTime:  executionTime,
		DocsExamined:   docsExamined,
		DocsReturned:   docsReturned,
		IndexUsed:      indexUsed,
		Timestamp:      time.Now().UTC(),
		CacheHit:       cacheHit,
	}

	// Analyze and add optimization suggestions
	metric.OptimizationSuggestions = pm.analyzeQueryPerformance(&metric)

	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Append metric, trim if over max
	pm.queryMetrics = append(pm.queryMetrics, metric)
	if len(pm.queryMetrics) > pm.maxMetrics {
		pm.queryMetrics = pm.queryMetrics[len(pm.queryMetrics)-pm.maxMetrics:]
	}

	// Update statistics
	pm.updateStatistics(&metric)

	// Track slow queries
	if executionTime > pm.slowQueryThreshold {
		pm.slowQueries = append(pm.slowQueries, metric)
		if len(pm.slowQueries) > pm.maxSlowQueries {
			pm.slowQueries = pm.slowQueries[len(pm.slowQueries)-pm.maxSlowQueries:]
		}
		pm.stats.SlowQueries++
		log.Printf("[PerfMonitor] Slow query detected: %s.%s took %.2fms", collection, operation, executionTime)
	}

	// Update query patterns
	pm.queryPatterns[querySignature]++

	// Update collection access patterns
	if _, ok := pm.collectionAccess[collection]; !ok {
		pm.collectionAccess[collection] = make(map[string]int)
	}
	pm.collectionAccess[collection][operation]++

	// Update index usage tracking
	if indexUsed != "" {
		if _, ok := pm.indexUsage[collection]; !ok {
			pm.indexUsage[collection] = make(map[string]int)
		}
		pm.indexUsage[collection][indexUsed]++
		pm.stats.IndexScans++
	} else {
		pm.stats.CollectionScans++
		if executionTime > 50 {
			log.Printf("[PerfMonitor] Slow collection scan: %s.%s (%.1fms) Query: %v",
				collection, operation, executionTime, query)
		}
	}
}

func (pm *PerformanceMonitor) createQuerySignature(collection string, operation string, query map[string]interface{}) string {
	var fieldPatterns []string

	for field, value := range query {
		if len(field) > 0 && field[0] == '$' {
			fieldPatterns = append(fieldPatterns, field)
		} else if valMap, ok := value.(map[string]interface{}); ok {
			var operators []string
			for k := range valMap {
				if len(k) > 0 && k[0] == '$' {
					operators = append(operators, k)
				}
			}
			if len(operators) > 0 {
				sort.Strings(operators)
				opStr := ""
				for i, op := range operators {
					if i > 0 {
						opStr += ":"
					}
					opStr += op
				}
				fieldPatterns = append(fieldPatterns, fmt.Sprintf("%s:%s", field, opStr))
			} else {
				fieldPatterns = append(fieldPatterns, field)
			}
		} else {
			fieldPatterns = append(fieldPatterns, field)
		}
	}

	sort.Strings(fieldPatterns)

	sig := fmt.Sprintf("%s.%s(", collection, operation)
	for i, fp := range fieldPatterns {
		if i > 0 {
			sig += ","
		}
		sig += fp
	}
	sig += ")"
	return sig
}

func (pm *PerformanceMonitor) extractIndexName(winningPlan map[string]interface{}) string {
	if inputStage, ok := winningPlan["inputStage"].(map[string]interface{}); ok {
		if stage, ok := inputStage["stage"].(string); ok && stage == "IXSCAN" {
			if name, ok := inputStage["indexName"].(string); ok {
				return name
			}
		}
	}
	if stage, ok := winningPlan["stage"].(string); ok && stage == "IXSCAN" {
		if name, ok := winningPlan["indexName"].(string); ok {
			return name
		}
	}
	return ""
}

func (pm *PerformanceMonitor) analyzeQueryPerformance(metric *QueryPerformanceMetric) []string {
	var suggestions []string

	if metric.ExecutionTime > pm.slowQueryThreshold {
		suggestions = append(suggestions,
			fmt.Sprintf("Query execution time (%.2fms) exceeds threshold", metric.ExecutionTime))
	}

	if metric.IndexUsed == "" {
		suggestions = append(suggestions, "Query performed collection scan - consider adding index")
	}

	if metric.DocsExamined > 0 {
		efficiency := float64(metric.DocsReturned) / float64(metric.DocsExamined)
		if efficiency < pm.efficiencyThreshold {
			suggestions = append(suggestions,
				fmt.Sprintf("Low efficiency ratio (%.3f) - query examines too many documents", efficiency))
		}
	}

	if metric.Collection == "users" {
		found := false
		for _, c := range []string{"user_id"} {
			if containsSubstring(metric.QuerySignature, c) {
				found = true
				break
			}
		}
		if !found {
			suggestions = append(suggestions, "Consider adding user_id filter for better performance")
		}
	}

	if metric.Operation == "find_many" && metric.DocsReturned > 1000 {
		suggestions = append(suggestions, "Large result set - consider adding limit or pagination")
	}

	return suggestions
}

func (pm *PerformanceMonitor) updateStatistics(metric *QueryPerformanceMetric) {
	pm.stats.TotalQueries++

	if metric.CacheHit {
		pm.stats.CacheHits++
	}

	// Update running average
	currentAvg := pm.stats.AvgExecutionTime
	total := pm.stats.TotalQueries
	pm.stats.AvgExecutionTime = (currentAvg*float64(total-1) + metric.ExecutionTime) / float64(total)
}

func (pm *PerformanceMonitor) monitoringLoop() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-pm.stopChan:
			return
		case <-ticker.C:
			pm.updateQPS()
			pm.analyzeTrends()
			pm.checkPerformanceAlerts()
		}
	}
}

func (pm *PerformanceMonitor) updateQPS() {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if len(pm.queryMetrics) < 2 {
		return
	}

	startIdx := len(pm.queryMetrics) - 60
	if startIdx < 0 {
		startIdx = 0
	}
	recent := pm.queryMetrics[startIdx:]

	if len(recent) >= 2 {
		timeSpan := recent[len(recent)-1].Timestamp.Sub(recent[0].Timestamp).Seconds()
		if timeSpan > 0 {
			pm.mu.RUnlock()
			pm.mu.Lock()
			pm.stats.QueriesPerSecond = float64(len(recent)) / timeSpan
			pm.mu.Unlock()
			pm.mu.RLock()
		}
	}
}

func (pm *PerformanceMonitor) analyzeTrends() {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if len(pm.queryMetrics) < 100 {
		return
	}

	recentStart := len(pm.queryMetrics) - 100
	recent := pm.queryMetrics[recentStart:]

	historicalEnd := recentStart
	historicalStart := historicalEnd - 400
	if historicalStart < 0 {
		historicalStart = 0
	}
	if historicalEnd <= historicalStart {
		return
	}
	historical := pm.queryMetrics[historicalStart:historicalEnd]

	if len(historical) == 0 {
		return
	}

	recentAvg := 0.0
	for _, m := range recent {
		recentAvg += m.ExecutionTime
	}
	recentAvg /= float64(len(recent))

	historicalAvg := 0.0
	for _, m := range historical {
		historicalAvg += m.ExecutionTime
	}
	historicalAvg /= float64(len(historical))

	if recentAvg > historicalAvg*1.5 {
		log.Printf("[PerfMonitor] Performance degradation detected: recent avg %.2fms vs historical %.2fms",
			recentAvg, historicalAvg)
	}
}

func (pm *PerformanceMonitor) checkPerformanceAlerts() {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	totalQueries := pm.stats.IndexScans + pm.stats.CollectionScans
	if totalQueries > 0 {
		collScanRatio := float64(pm.stats.CollectionScans) / float64(totalQueries)
		if collScanRatio > 0.3 {
			log.Printf("[PerfMonitor] High collection scan ratio: %.2f", collScanRatio)
		}
	}

	if pm.stats.TotalQueries > 100 {
		cacheHitRate := float64(pm.stats.CacheHits) / float64(pm.stats.TotalQueries)
		if cacheHitRate < 0.2 {
			log.Printf("[PerfMonitor] Low cache hit rate: %.2f", cacheHitRate)
		}
	}
}

// GetPerformanceReport generates a comprehensive performance report
func (pm *PerformanceMonitor) GetPerformanceReport() map[string]interface{} {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	// Recent metrics
	recentStart := 0
	if len(pm.queryMetrics) > 1000 {
		recentStart = len(pm.queryMetrics) - 1000
	}
	recentMetrics := pm.queryMetrics[recentStart:]

	slowCount := 0
	for _, m := range recentMetrics {
		if m.ExecutionTime > pm.slowQueryThreshold {
			slowCount++
		}
	}
	slowQueryRatio := 0.0
	if len(recentMetrics) > 0 {
		slowQueryRatio = float64(slowCount) / float64(len(recentMetrics))
	}

	// Top slow queries (top 10 by execution time)
	sortedSlow := make([]QueryPerformanceMetric, len(pm.slowQueries))
	copy(sortedSlow, pm.slowQueries)
	sort.Slice(sortedSlow, func(i, j int) bool {
		return sortedSlow[i].ExecutionTime > sortedSlow[j].ExecutionTime
	})
	if len(sortedSlow) > 10 {
		sortedSlow = sortedSlow[:10]
	}

	topSlowQueries := make([]map[string]interface{}, 0, len(sortedSlow))
	for _, q := range sortedSlow {
		topSlowQueries = append(topSlowQueries, map[string]interface{}{
			"signature":      q.QuerySignature,
			"execution_time": q.ExecutionTime,
			"docs_examined":  q.DocsExamined,
			"docs_returned":  q.DocsReturned,
			"index_used":     q.IndexUsed,
			"suggestions":    q.OptimizationSuggestions,
		})
	}

	// Top query patterns (top 10 by count)
	type patternEntry struct {
		Pattern string
		Count   int
	}
	var patterns []patternEntry
	for p, c := range pm.queryPatterns {
		patterns = append(patterns, patternEntry{p, c})
	}
	sort.Slice(patterns, func(i, j int) bool {
		return patterns[i].Count > patterns[j].Count
	})
	if len(patterns) > 10 {
		patterns = patterns[:10]
	}
	topPatterns := make([]map[string]interface{}, 0, len(patterns))
	for _, p := range patterns {
		topPatterns = append(topPatterns, map[string]interface{}{
			"pattern": p.Pattern,
			"count":   p.Count,
		})
	}

	// Index usage summary
	indexUsageSummary := make(map[string]interface{})
	for collection, indexes := range pm.indexUsage {
		totalUsage := 0
		for _, count := range indexes {
			totalUsage += count
		}

		// Sort indexes by usage
		type indexEntry struct {
			Name  string
			Count int
		}
		var sortedIndexes []indexEntry
		for name, count := range indexes {
			sortedIndexes = append(sortedIndexes, indexEntry{name, count})
		}
		sort.Slice(sortedIndexes, func(i, j int) bool {
			return sortedIndexes[i].Count > sortedIndexes[j].Count
		})
		indexMap := make(map[string]int)
		for _, idx := range sortedIndexes {
			indexMap[idx.Name] = idx.Count
		}

		indexUsageSummary[collection] = map[string]interface{}{
			"total_index_usage": totalUsage,
			"indexes":           indexMap,
		}
	}

	// Collection access copy
	collAccess := make(map[string]interface{})
	for col, ops := range pm.collectionAccess {
		opsCopy := make(map[string]int)
		for op, count := range ops {
			opsCopy[op] = count
		}
		collAccess[col] = opsCopy
	}

	totalQ := pm.stats.TotalQueries
	cacheHitRate := 0.0
	if totalQ > 0 {
		cacheHitRate = float64(pm.stats.CacheHits) / float64(totalQ)
	}
	indexScanRatio := 0.0
	if totalQ > 0 {
		indexScanRatio = float64(pm.stats.IndexScans) / float64(totalQ)
	}

	return map[string]interface{}{
		"summary": map[string]interface{}{
			"total_queries":      pm.stats.TotalQueries,
			"avg_execution_time": pm.stats.AvgExecutionTime,
			"queries_per_second": pm.stats.QueriesPerSecond,
			"slow_query_ratio":   slowQueryRatio,
			"cache_hit_rate":     cacheHitRate,
			"index_scan_ratio":   indexScanRatio,
		},
		"top_slow_queries":  topSlowQueries,
		"query_patterns":    topPatterns,
		"index_usage":       indexUsageSummary,
		"collection_access": collAccess,
	}
}

// GetOptimizationRecommendations returns optimization recommendations based on collected data
func (pm *PerformanceMonitor) GetOptimizationRecommendations() []map[string]interface{} {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	var recommendations []map[string]interface{}

	// Analyze slow queries for index recommendations
	slowPatterns := make(map[string]int)
	for _, q := range pm.slowQueries {
		if q.IndexUsed == "" {
			slowPatterns[q.QuerySignature]++
		}
	}

	for pattern, count := range slowPatterns {
		if count >= 3 {
			recommendations = append(recommendations, map[string]interface{}{
				"type":             "missing_index",
				"priority":         "high",
				"description":      fmt.Sprintf("Add index for pattern: %s", pattern),
				"affected_queries": count,
				"estimated_impact": "high",
			})
		}
	}

	// Check for collections with high collection scan ratios
	for collection, accessPatterns := range pm.collectionAccess {
		totalAccess := 0
		for _, count := range accessPatterns {
			totalAccess += count
		}
		if totalAccess > 100 {
			indexUsage := pm.indexUsage[collection]
			totalIndexUsage := 0
			for _, count := range indexUsage {
				totalIndexUsage += count
			}
			if float64(totalIndexUsage)/float64(totalAccess) < 0.5 {
				recommendations = append(recommendations, map[string]interface{}{
					"type":             "low_index_usage",
					"priority":         "medium",
					"description":      fmt.Sprintf("Collection '%s' has low index usage ratio", collection),
					"collection":       collection,
					"estimated_impact": "medium",
				})
			}
		}
	}

	// Check for potential caching opportunities
	recentStart := 0
	if len(pm.queryMetrics) > 1000 {
		recentStart = len(pm.queryMetrics) - 1000
	}
	recentMetrics := pm.queryMetrics[recentStart:]

	var frequentPatterns []string
	for p, c := range pm.queryPatterns {
		if c >= 10 {
			frequentPatterns = append(frequentPatterns, p)
		}
	}

	type uncachedEntry struct {
		Pattern string
		Count   int
		HitRate float64
	}
	var uncachedFrequent []uncachedEntry

	for _, pattern := range frequentPatterns {
		patternCount := 0
		cacheHits := 0
		for _, m := range recentMetrics {
			if m.QuerySignature == pattern {
				patternCount++
				if m.CacheHit {
					cacheHits++
				}
			}
		}
		if patternCount > 0 {
			hitRate := float64(cacheHits) / float64(patternCount)
			if hitRate < 0.3 {
				uncachedFrequent = append(uncachedFrequent, uncachedEntry{pattern, patternCount, hitRate})
			}
		}
	}

	// Limit to top 5
	if len(uncachedFrequent) > 5 {
		sort.Slice(uncachedFrequent, func(i, j int) bool {
			return uncachedFrequent[i].Count > uncachedFrequent[j].Count
		})
		uncachedFrequent = uncachedFrequent[:5]
	}

	for _, entry := range uncachedFrequent {
		recommendations = append(recommendations, map[string]interface{}{
			"type":              "caching_opportunity",
			"priority":          "low",
			"description":       fmt.Sprintf("Increase cache TTL for frequent pattern: %s", entry.Pattern),
			"query_count":       entry.Count,
			"current_hit_rate":  entry.HitRate,
			"estimated_impact":  "medium",
		})
	}

	// Sort by priority
	priorityOrder := map[string]int{"high": 3, "medium": 2, "low": 1}
	sort.Slice(recommendations, func(i, j int) bool {
		pi := priorityOrder[recommendations[i]["priority"].(string)]
		pj := priorityOrder[recommendations[j]["priority"].(string)]
		return pi > pj
	})

	return recommendations
}

// ClearMetrics clears all collected metrics
func (pm *PerformanceMonitor) ClearMetrics() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.queryMetrics = pm.queryMetrics[:0]
	pm.slowQueries = pm.slowQueries[:0]
	pm.indexUsage = make(map[string]map[string]int)
	pm.queryPatterns = make(map[string]int)
	pm.collectionAccess = make(map[string]map[string]int)
	pm.stats = PerfStats{}

	log.Println("[PerfMonitor] Cleared all performance metrics")
}

// ─── Helper ───

func containsSubstring(s, substr string) bool {
	if len(substr) > len(s) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// Global performance monitor instance
var _performanceMonitor *PerformanceMonitor

func GetPerformanceMonitor(dbManager *DatabaseManager) *PerformanceMonitor {
	if _performanceMonitor == nil {
		_performanceMonitor = NewPerformanceMonitor(dbManager)
	}
	return _performanceMonitor
}