package database

import (
	"log"
	"time"
)

// DatabaseManager is the enhanced database adapter with caching, optimization, and monitoring
type DatabaseManager struct {
	instanceName         string
	cache                *IntelligentCache
	queryOptimizer       *QueryOptimizer
	aggregationOptimizer *AggregationOptimizer
	performanceMonitor   *PerformanceMonitor
}

func NewDatabaseManager() *DatabaseManager {
	return &DatabaseManager{
		instanceName: "adapter",
		cache:        GetCache(),
	}
}

func (dm *DatabaseManager) Initialize() error {
	// Start caching
	dm.cache.Start()

	// Initialize optimizers
	dm.queryOptimizer = GetQueryOptimizer(dm)
	dm.aggregationOptimizer = GetAggregationOptimizer(dm)
	dm.performanceMonitor = GetPerformanceMonitor(dm)

	// Start performance monitoring
	dm.performanceMonitor.StartMonitoring()

	log.Printf("[%s] Database adapter ready with full optimization suite", dm.instanceName)
	return nil
}

func (dm *DatabaseManager) FindOne(collection string, query map[string]interface{}, projection map[string]interface{}) (map[string]interface{}, error) {
	startTime := time.Now()

	// Try cache first
	cached := dm.cache.Get(collection, "find_one", query, projection, nil, 0, 0)
	if cached != nil {
		executionTime := float64(time.Since(startTime).Milliseconds())
		if dm.performanceMonitor != nil {
			dm.performanceMonitor.RecordQuery(collection, "find_one", query, executionTime, 1, true, nil)
		}
		if result, ok := cached.(map[string]interface{}); ok {
			return result, nil
		}
	}

	// Optimize query
	if dm.queryOptimizer != nil {
		optimization := dm.queryOptimizer.OptimizeQuery(collection, "find_one", query, projection, nil, 0, 0)
		query = optimization.Query
		if optimization.Projection != nil {
			projection = optimization.Projection
		}
	}

	// Execute query
	result, err := GlobalFindOne(collection, query, projection)
	executionTime := float64(time.Since(startTime).Milliseconds())

	// Get explain data for slow queries
	var explainData map[string]interface{}
	if executionTime > 100 {
		log.Printf("[%s] Slow query on %s.find_one: %.2fms", dm.instanceName, collection, executionTime)
	}

	// Record performance metrics
	if dm.performanceMonitor != nil {
		resultCount := 0
		if result != nil {
			resultCount = 1
		}
		dm.performanceMonitor.RecordQuery(collection, "find_one", query, executionTime, resultCount, false, explainData)
	}

	// Cache the result
	if err == nil && result != nil {
		dm.cache.Set(collection, "find_one", query, result, projection, nil, 0, 0)
	}

	return result, err
}

func (dm *DatabaseManager) UpdateOne(collection string, filter map[string]interface{}, update map[string]interface{}, upsert bool) error {
	err := GlobalUpdateOne(collection, filter, update, upsert)
	if err != nil {
		return err
	}

	// Invalidate related cache entries
	dm.cache.InvalidateByWrite(collection, "update_one", filter, update)
	return nil
}

func (dm *DatabaseManager) UpdateMany(collection string, filter map[string]interface{}, update map[string]interface{}, upsert bool) error {
	err := GlobalUpdateMany(collection, filter, update, upsert)
	if err != nil {
		return err
	}

	dm.cache.InvalidateByWrite(collection, "update_many", filter, update)
	return nil
}

func (dm *DatabaseManager) InsertOne(collection string, document map[string]interface{}) error {
	return GlobalInsertOne(collection, document)
}

func (dm *DatabaseManager) DeleteOne(collection string, filter map[string]interface{}) error {
	return GlobalDeleteOne(collection, filter)
}

func (dm *DatabaseManager) FindMany(collection string, query map[string]interface{}, projection map[string]interface{}, sort []SortField, limit int, skip int) ([]map[string]interface{}, error) {
	// Try cache first
	cached := dm.cache.Get(collection, "find_many", query, projection, sort, limit, skip)
	if cached != nil {
		if result, ok := cached.([]map[string]interface{}); ok {
			return result, nil
		}
	}

	result, err := GlobalFindMany(collection, query, projection, sort, limit, skip)
	if err != nil {
		return nil, err
	}

	// Cache if not too large
	if len(result) > 0 && len(result) <= 1000 {
		dm.cache.Set(collection, "find_many", query, result, projection, sort, limit, skip)
	}

	return result, nil
}

func (dm *DatabaseManager) Aggregate(collection string, pipeline []map[string]interface{}) ([]map[string]interface{}, error) {
	return GlobalAggregate(collection, pipeline)
}

func (dm *DatabaseManager) CountDocuments(collection string, filter map[string]interface{}) (int64, error) {
	return GlobalCountDocuments(collection, filter)
}

func (dm *DatabaseManager) BulkWrite(collection string, requests []WriteModel, ordered bool) (*BulkWriteResult, error) {
	return GlobalBulkWrite(collection, requests, ordered)
}

func (dm *DatabaseManager) InsertMany(collection string, documents []map[string]interface{}, ordered bool) (*BulkWriteResult, error) {
	return GlobalInsertMany(collection, documents, ordered)
}

func (dm *DatabaseManager) UpdateManyBulk(collection string, operations []BulkUpdateOp, ordered bool) (*BulkWriteResult, error) {
	return GlobalUpdateManyBulk(collection, operations, ordered)
}

func (dm *DatabaseManager) CreateBatchProcessor(collection string, batchSize int, ordered bool) *BatchProcessor {
	return NewBatchProcessor(collection, batchSize, ordered)
}

// Optimized query methods

func (dm *DatabaseManager) FindUserByID(userID int64, projection map[string]interface{}) (map[string]interface{}, error) {
	if projection == nil {
		projection = map[string]interface{}{
			"user_id": 1, "current_username": 1, "current_displayname": 1,
			"current_avatar": 1, "last_seen": 1, "first_seen": 1,
		}
	}
	return dm.FindOne("users", map[string]interface{}{"user_id": userID}, projection)
}

func (dm *DatabaseManager) FindRecentMessages(userID int64, limit int) ([]map[string]interface{}, error) {
	projection := map[string]interface{}{
		"user_id": 1, "content": 1, "created_at": 1, "guild_id": 1, "channel_id": 1,
	}
	return dm.FindMany(
		"user_messages",
		map[string]interface{}{"user_id": userID},
		projection,
		[]SortField{{Field: "created_at", Descending: true}},
		limit,
		0,
	)
}

func (dm *DatabaseManager) FindUserMentions(userID int64, days int, limit int) ([]map[string]interface{}, error) {
	cutoff := time.Now().UTC().AddDate(0, 0, -days)
	projection := map[string]interface{}{
		"target_id": 1, "mentioner_id": 1, "created_at": 1, "content": 1,
	}
	return dm.FindMany(
		"mentions",
		map[string]interface{}{
			"target_id":  userID,
			"created_at": map[string]interface{}{"$gte": cutoff},
		},
		projection,
		[]SortField{{Field: "created_at", Descending: true}},
		limit,
		0,
	)
}

// Performance and optimization methods

func (dm *DatabaseManager) GetPerformanceReport() map[string]interface{} {
	if dm.performanceMonitor != nil {
		return dm.performanceMonitor.GetPerformanceReport()
	}
	return map[string]interface{}{"error": "Performance monitoring not available"}
}

func (dm *DatabaseManager) GetOptimizationRecommendations() []map[string]interface{} {
	if dm.performanceMonitor != nil {
		return dm.performanceMonitor.GetOptimizationRecommendations()
	}
	return nil
}

func (dm *DatabaseManager) GetUserActivitySummary(userID int64, days int) ([]map[string]interface{}, error) {
	if dm.aggregationOptimizer != nil {
		pipeline := dm.aggregationOptimizer.UserActivitySummary(userID, days)
		return dm.aggregationOptimizer.ExecuteOptimizedAggregation("user_messages", pipeline, nil)
	}
	return nil, nil
}

func (dm *DatabaseManager) GetGuildLeaderboard(guildID int64, days int, limit int) ([]map[string]interface{}, error) {
	if dm.aggregationOptimizer != nil {
		pipeline := dm.aggregationOptimizer.GuildUserLeaderboard(guildID, days, limit)
		return dm.aggregationOptimizer.ExecuteOptimizedAggregation("user_messages", pipeline, nil)
	}
	return nil, nil
}

func (dm *DatabaseManager) GetCacheStats() map[string]interface{} {
	return dm.cache.GetStats()
}

func (dm *DatabaseManager) ClearCache(collection string) {
	if collection != "" {
		dm.cache.InvalidateCollection(collection)
	} else {
		dm.cache.ClearAll()
	}
}

func (dm *DatabaseManager) GetDB() interface{} {
	return GetGlobalDB()
}

func (dm *DatabaseManager) IsActive() bool {
	return IsGlobalDBActive()
}

func (dm *DatabaseManager) InstanceID() string {
	return dm.instanceName
}

func (dm *DatabaseManager) Close() {
	if dm.performanceMonitor != nil {
		dm.performanceMonitor.StopMonitoring()
	}
	dm.cache.Stop()
	log.Printf("[%s] Adapter closed", dm.instanceName)
}

// ParseBulkResult parses bulk write results
func ParseBulkResult(result *BulkWriteResult, operationType string) map[string]interface{} {
	if result == nil {
		return map[string]interface{}{"success": false, "count": 0, "batches": 0}
	}
	count := result.InsertedCount + result.ModifiedCount + result.DeletedCount + result.UpsertedCount
	return map[string]interface{}{
		"success":        true,
		"count":          count,
		"batches":        1,
		"operation_type": operationType,
	}
}