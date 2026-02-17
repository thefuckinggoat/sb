package database

import (
	"fmt"
	"log"
	"strconv"
	"time"
)

// OptimizedQuery holds the result of query optimization
type OptimizedQuery struct {
	Query         map[string]interface{}
	Projection    map[string]interface{}
	Sort          []SortField
	Limit         int
	Skip          int
	Hint          interface{}
	Optimizations []string
}

// QueryOptimizer ensures optimal index utilization and query performance
type QueryOptimizer struct {
	dbManager          *DatabaseManager
	indexHints         map[string]map[string]interface{}
	optimalProjections map[string]map[string]interface{}
}

func NewQueryOptimizer(dbManager *DatabaseManager) *QueryOptimizer {
	qo := &QueryOptimizer{
		dbManager: dbManager,
		indexHints: map[string]map[string]interface{}{
			"users": {
				"user_id_lookup":  map[string]interface{}{"user_id": 1},
				"username_search": "name_search_text_index",
				"recent_activity": map[string]interface{}{"last_seen": -1, "user_id": 1},
				"guild_activity":  map[string]interface{}{"guild_count": -1, "last_seen": -1},
			},
			"user_messages": {
				"user_messages_recent": map[string]interface{}{"user_id": 1, "created_at": -1},
				"channel_messages":     map[string]interface{}{"guild_id": 1, "channel_id": 1, "created_at": -1},
				"message_content_search": "message_content_text",
			},
			"deleted_messages": {
				"user_deleted":    map[string]interface{}{"user_id": 1, "deleted_at": -1},
				"channel_deleted": map[string]interface{}{"guild_id": 1, "channel_id": 1, "deleted_at": -1},
				"recent_deleted":  map[string]interface{}{"deleted_at": -1},
			},
			"edited_messages": {
				"user_edited":    map[string]interface{}{"user_id": 1, "edited_at": -1},
				"channel_edited": map[string]interface{}{"guild_id": 1, "channel_id": 1, "edited_at": -1},
			},
			"mentions": {
				"user_mentions":     map[string]interface{}{"target_id": 1, "created_at": -1},
				"mentioner_history": map[string]interface{}{"mentioner_id": 1, "created_at": -1},
				"channel_mentions":  map[string]interface{}{"guild_id": 1, "channel_id": 1, "created_at": -1},
			},
		},
		optimalProjections: map[string]map[string]interface{}{
			"user_basic": {
				"user_id": 1, "current_username": 1, "current_displayname": 1,
				"current_avatar": 1, "last_seen": 1, "first_seen": 1,
			},
			"user_search": {
				"user_id": 1, "current_username": 1, "current_displayname": 1,
				"username_history":    map[string]interface{}{"$slice": 5},
				"displayname_history": map[string]interface{}{"$slice": 5},
			},
			"message_basic": {
				"message_id": 1, "user_id": 1, "content": 1, "created_at": 1,
				"guild_id": 1, "channel_id": 1,
			},
			"message_minimal": {
				"user_id": 1, "created_at": 1, "guild_id": 1,
			},
			"mention_basic": {
				"target_id": 1, "mentioner_id": 1, "created_at": 1,
				"content": 1, "message_id": 1,
			},
		},
	}
	return qo
}

// OptimizeQuery optimizes a query by adding hints, improving projections, and rewriting conditions
func (qo *QueryOptimizer) OptimizeQuery(collection string, operation string,
	query map[string]interface{}, projection map[string]interface{},
	sort []SortField, limit int, skip int) *OptimizedQuery {

	optimized := &OptimizedQuery{
		Query:         copyMap(query),
		Projection:    copyMap(projection),
		Sort:          sort,
		Limit:         limit,
		Skip:          skip,
		Hint:          nil,
		Optimizations: []string{},
	}

	// Add index hint
	hint := qo.determineIndexHint(collection, query, sort)
	if hint != nil {
		optimized.Hint = hint
		optimized.Optimizations = append(optimized.Optimizations, fmt.Sprintf("Added index hint: %v", hint))
	}

	// Optimize projection
	if projection == nil {
		suggested := qo.suggestProjection(collection, operation)
		if suggested != nil {
			optimized.Projection = suggested
			optimized.Optimizations = append(optimized.Optimizations, "Added optimal projection")
		}
	}

	// Optimize query conditions
	optimizedQuery := qo.optimizeQueryConditions(collection, query)
	if len(optimizedQuery) != len(query) {
		optimized.Query = optimizedQuery
		optimized.Optimizations = append(optimized.Optimizations, "Optimized query conditions")
	}

	// Optimize sort
	if sort != nil {
		optimizedSort := qo.optimizeSortConditions(collection, sort, query)
		optimized.Sort = optimizedSort
	}

	return optimized
}

func (qo *QueryOptimizer) determineIndexHint(collection string, query map[string]interface{}, sort []SortField) interface{} {
	collectionHints, ok := qo.indexHints[collection]
	if !ok {
		return nil
	}

	queryFields := make(map[string]struct{})
	for k := range query {
		queryFields[k] = struct{}{}
	}

	// User ID lookups
	if _, ok := queryFields["user_id"]; ok && len(queryFields) == 1 {
		return collectionHints["user_id_lookup"]
	}

	// Text search queries
	for _, val := range query {
		if strVal, ok := val.(string); ok {
			if strVal == "$text" {
				return collectionHints["username_search"]
			}
		}
	}
	for field := range queryFields {
		if field == "current_username" || field == "current_displayname" {
			return collectionHints["username_search"]
		}
	}

	// Time-based queries with user
	if _, hasUser := queryFields["user_id"]; hasUser && len(sort) > 0 {
		sortField := sort[0].Field
		switch sortField {
		case "created_at":
			return collectionHints["user_messages_recent"]
		case "last_seen":
			return collectionHints["recent_activity"]
		case "deleted_at":
			return collectionHints["user_deleted"]
		case "edited_at":
			return collectionHints["user_edited"]
		}
	}

	// Channel-based queries
	_, hasGuild := queryFields["guild_id"]
	_, hasChannel := queryFields["channel_id"]
	if hasGuild && hasChannel {
		switch collection {
		case "user_messages":
			return collectionHints["channel_messages"]
		case "deleted_messages":
			return collectionHints["channel_deleted"]
		case "edited_messages":
			return collectionHints["channel_edited"]
		case "mentions":
			return collectionHints["channel_mentions"]
		}
	}

	// Mention-specific queries
	if collection == "mentions" {
		if _, ok := queryFields["target_id"]; ok {
			return collectionHints["user_mentions"]
		}
		if _, ok := queryFields["mentioner_id"]; ok {
			return collectionHints["mentioner_history"]
		}
	}

	return nil
}

func (qo *QueryOptimizer) suggestProjection(collection string, operation string) map[string]interface{} {
	projectionMap := map[string]string{
		"users:find_one":              "user_basic",
		"users:find_many":             "user_basic",
		"users:search":                "user_search",
		"user_messages:find_many":     "message_basic",
		"user_messages:recent":        "message_minimal",
		"mentions:find_many":          "mention_basic",
		"deleted_messages:find_many":  "message_basic",
		"edited_messages:find_many":   "message_basic",
	}

	key := fmt.Sprintf("%s:%s", collection, operation)
	projectionKey, ok := projectionMap[key]
	if !ok {
		return nil
	}

	return qo.optimalProjections[projectionKey]
}

func (qo *QueryOptimizer) optimizeQueryConditions(collection string, query map[string]interface{}) map[string]interface{} {
	optimized := copyMap(query)

	// Convert string IDs to integers for better index performance
	idFields := []string{"user_id", "guild_id", "channel_id", "message_id", "target_id", "mentioner_id"}
	for _, field := range idFields {
		if val, ok := optimized[field]; ok {
			if strVal, ok := val.(string); ok {
				if intVal, err := strconv.ParseInt(strVal, 10, 64); err == nil {
					optimized[field] = intVal
				}
			}
		}
	}

	// Optimize date range queries
	dateFieldMap := map[string]string{
		"user_messages":    "created_at",
		"deleted_messages": "deleted_at",
		"edited_messages":  "edited_at",
		"mentions":         "created_at",
	}

	if dateField, ok := dateFieldMap[collection]; ok {
		if _, exists := optimized[dateField]; !exists {
			// Add recent data filter (last 30 days) to improve index utilization
			recentDate := time.Now().UTC().AddDate(0, 0, -30)
			optimized[dateField] = map[string]interface{}{"$gte": recentDate}
		}
	}

	// Optimize text search queries for users
	if collection == "users" {
		textFields := []string{"current_username", "current_displayname"}
		for _, field := range textFields {
			if val, ok := optimized[field]; ok {
				if strVal, ok := val.(string); ok {
					if len(strVal) > 0 && strVal[0] != '$' {
						optimized[field] = map[string]interface{}{
							"$regex":   strVal,
							"$options": "i",
						}
					}
				}
			}
		}
	}

	return optimized
}

func (qo *QueryOptimizer) optimizeSortConditions(collection string, sort []SortField, query map[string]interface{}) []SortField {
	if len(sort) == 0 {
		return sort
	}

	// For user queries with time-based sorting, compound index handles it
	messageCollections := map[string]bool{
		"user_messages":    true,
		"deleted_messages": true,
		"edited_messages":  true,
	}
	if messageCollections[collection] {
		if _, hasUser := query["user_id"]; hasUser {
			timeFields := map[string]bool{"created_at": true, "deleted_at": true, "edited_at": true}
			if timeFields[sort[0].Field] {
				// Compound index (user_id, created_at) will be used automatically
			}
		}
	}

	// For mentions, compound index handles target_id + created_at
	if collection == "mentions" {
		if _, hasTarget := query["target_id"]; hasTarget {
			if sort[0].Field == "created_at" {
				// Compound index (target_id, created_at) will be used
			}
		}
	}

	return sort
}

// ExplainQuery returns query execution plan information
func (qo *QueryOptimizer) ExplainQuery(collection string, query map[string]interface{},
	projection map[string]interface{}, sort []SortField, limit int) (map[string]interface{}, error) {

	db := GetGlobalDB()
	if db == nil {
		return nil, fmt.Errorf("database not available")
	}

	// Note: The Go MongoDB driver doesn't have a direct .explain() on cursors like Python.
	// You'd use the runCommand approach instead.
	// This is a simplified version that returns basic info.
	log.Printf("[QueryOptimizer] Explain requested for %s (Go driver has limited explain support via cursor)", collection)

	return map[string]interface{}{
		"collection": collection,
		"query":      query,
		"note":       "Use MongoDB shell or Compass for detailed explain plans",
	}, nil
}

// GetIndexSuggestions analyzes queries and suggests new indexes
func (qo *QueryOptimizer) GetIndexSuggestions(collection string, recentQueries []map[string]interface{}) []map[string]interface{} {
	var suggestions []map[string]interface{}

	fieldCombinations := make(map[string]int)
	sortPatterns := make(map[string]int)

	for _, queryInfo := range recentQueries {
		query, _ := queryInfo["query"].(map[string]interface{})
		sort, _ := queryInfo["sort"].([]SortField)

		// Track field combinations
		var fields []string
		for k := range query {
			fields = append(fields, k)
		}
		key := fmt.Sprintf("%v", fields)
		fieldCombinations[key]++

		// Track sort patterns
		if len(sort) > 0 {
			sortKey := fmt.Sprintf("%v", sort)
			sortPatterns[sortKey]++
		}
	}

	// Suggest compound indexes for common field combinations
	for fields, count := range fieldCombinations {
		if count >= 5 {
			suggestions = append(suggestions, map[string]interface{}{
				"type":        "compound_index",
				"collection":  collection,
				"fields":      fields,
				"usage_count": count,
				"reason":      fmt.Sprintf("Frequently queried fields: %s", fields),
			})
		}
	}

	// Suggest indexes for common sort patterns
	for pattern, count := range sortPatterns {
		if count >= 3 {
			suggestions = append(suggestions, map[string]interface{}{
				"type":        "sort_index",
				"collection":  collection,
				"fields":      pattern,
				"usage_count": count,
				"reason":      "Frequently used sort pattern",
			})
		}
	}

	return suggestions
}

// CreateOptimizedAggregationPipeline creates an optimized aggregation pipeline skeleton
func (qo *QueryOptimizer) CreateOptimizedAggregationPipeline(collection string, operations []string) []map[string]interface{} {
	var pipeline []map[string]interface{}

	opSet := make(map[string]bool)
	for _, op := range operations {
		opSet[op] = true
	}

	if opSet["filter"] {
		pipeline = append(pipeline, map[string]interface{}{"$match": map[string]interface{}{}})
	}
	if opSet["sort"] {
		pipeline = append(pipeline, map[string]interface{}{"$sort": map[string]interface{}{}})
	}
	if opSet["limit"] {
		pipeline = append(pipeline, map[string]interface{}{"$limit": 1000})
	}
	if opSet["project"] {
		pipeline = append(pipeline, map[string]interface{}{"$project": map[string]interface{}{}})
	}
	if opSet["group"] {
		pipeline = append(pipeline, map[string]interface{}{"$group": map[string]interface{}{}})
	}

	return pipeline
}

// ─── Helpers ───

func copyMap(m map[string]interface{}) map[string]interface{} {
	if m == nil {
		return nil
	}
	cp := make(map[string]interface{}, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}

// Global optimizer instance
var _optimizer *QueryOptimizer

func GetQueryOptimizer(dbManager *DatabaseManager) *QueryOptimizer {
	if _optimizer == nil {
		_optimizer = NewQueryOptimizer(dbManager)
	}
	return _optimizer
}