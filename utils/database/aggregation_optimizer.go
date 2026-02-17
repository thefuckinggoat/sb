package database

import (
	"log"
	"time"
)

// AggregationOptimizer provides pre-built, optimized pipelines for common Discord selfbot analytics
type AggregationOptimizer struct {
	dbManager *DatabaseManager
}

func NewAggregationOptimizer(dbManager *DatabaseManager) *AggregationOptimizer {
	return &AggregationOptimizer{dbManager: dbManager}
}

// UserActivitySummary returns an optimized pipeline for user activity summary
func (ao *AggregationOptimizer) UserActivitySummary(userID int64, days int) []map[string]interface{} {
	cutoff := time.Now().UTC().AddDate(0, 0, -days)

	return []map[string]interface{}{
		// Stage 1: Match user and recent messages
		{"$match": map[string]interface{}{
			"user_id":    userID,
			"created_at": map[string]interface{}{"$gte": cutoff},
		}},
		// Stage 2: Group by day and guild
		{"$group": map[string]interface{}{
			"_id": map[string]interface{}{
				"date":     map[string]interface{}{"$dateToString": map[string]interface{}{"format": "%Y-%m-%d", "date": "$created_at"}},
				"guild_id": "$guild_id",
			},
			"message_count":   map[string]interface{}{"$sum": 1},
			"unique_channels": map[string]interface{}{"$addToSet": "$channel_id"},
			"first_message":   map[string]interface{}{"$min": "$created_at"},
			"last_message":    map[string]interface{}{"$max": "$created_at"},
		}},
		// Stage 3: Calculate per-day metrics
		{"$group": map[string]interface{}{
			"_id":            "$_id.date",
			"total_messages": map[string]interface{}{"$sum": "$message_count"},
			"guilds_active":  map[string]interface{}{"$addToSet": "$_id.guild_id"},
			"total_channels": map[string]interface{}{"$sum": map[string]interface{}{"$size": "$unique_channels"}},
			"activity_span": map[string]interface{}{
				"$max": map[string]interface{}{
					"$subtract": []interface{}{"$last_message", "$first_message"},
				},
			},
		}},
		// Stage 4: Add computed fields
		{"$addFields": map[string]interface{}{
			"guild_count":    map[string]interface{}{"$size": "$guilds_active"},
			"activity_hours": map[string]interface{}{"$divide": []interface{}{"$activity_span", 3600000}},
		}},
		// Stage 5: Sort by date
		{"$sort": map[string]interface{}{"_id": -1}},
		// Stage 6: Project final format
		{"$project": map[string]interface{}{
			"date":           "$_id",
			"messages":       "$total_messages",
			"guilds":         "$guild_count",
			"channels":       "$total_channels",
			"activity_hours": map[string]interface{}{"$round": []interface{}{"$activity_hours", 2}},
			"_id":            0,
		}},
	}
}

// GuildUserLeaderboard returns an optimized pipeline for guild user activity leaderboard
func (ao *AggregationOptimizer) GuildUserLeaderboard(guildID int64, days int, limit int) []map[string]interface{} {
	cutoff := time.Now().UTC().AddDate(0, 0, -days)

	return []map[string]interface{}{
		// Stage 1: Match guild and recent messages
		{"$match": map[string]interface{}{
			"guild_id":   guildID,
			"created_at": map[string]interface{}{"$gte": cutoff},
		}},
		// Stage 2: Group by user
		{"$group": map[string]interface{}{
			"_id":             "$user_id",
			"message_count":   map[string]interface{}{"$sum": 1},
			"unique_channels": map[string]interface{}{"$addToSet": "$channel_id"},
			"first_message":   map[string]interface{}{"$min": "$created_at"},
			"last_message":    map[string]interface{}{"$max": "$created_at"},
			"total_chars":     map[string]interface{}{"$sum": map[string]interface{}{"$strLenCP": "$content"}},
		}},
		// Stage 3: Add computed metrics
		{"$addFields": map[string]interface{}{
			"channel_count":      map[string]interface{}{"$size": "$unique_channels"},
			"avg_message_length": map[string]interface{}{"$divide": []interface{}{"$total_chars", "$message_count"}},
			"activity_span_hours": map[string]interface{}{
				"$divide": []interface{}{
					map[string]interface{}{"$subtract": []interface{}{"$last_message", "$first_message"}},
					3600000,
				},
			},
		}},
		// Stage 4: Sort by message count
		{"$sort": map[string]interface{}{"message_count": -1}},
		// Stage 5: Limit results
		{"$limit": limit},
		// Stage 6: Lookup user details
		{"$lookup": map[string]interface{}{
			"from":         "users",
			"localField":   "_id",
			"foreignField": "user_id",
			"as":           "user_info",
			"pipeline": []map[string]interface{}{
				{"$project": map[string]interface{}{
					"current_username":    1,
					"current_displayname": 1,
					"current_avatar":      1,
				}},
			},
		}},
		// Stage 7: Project final format
		{"$project": map[string]interface{}{
			"user_id":        "$_id",
			"username":       map[string]interface{}{"$arrayElemAt": []interface{}{"$user_info.current_username", 0}},
			"displayname":    map[string]interface{}{"$arrayElemAt": []interface{}{"$user_info.current_displayname", 0}},
			"messages":       "$message_count",
			"channels":       "$channel_count",
			"avg_length":     map[string]interface{}{"$round": []interface{}{"$avg_message_length", 1}},
			"activity_hours": map[string]interface{}{"$round": []interface{}{"$activity_span_hours", 1}},
			"_id":            0,
		}},
	}
}

// MessageTimelineAnalysis returns an optimized pipeline for hourly message breakdown
func (ao *AggregationOptimizer) MessageTimelineAnalysis(userID int64, days int) []map[string]interface{} {
	cutoff := time.Now().UTC().AddDate(0, 0, -days)

	return []map[string]interface{}{
		// Stage 1: Match user and recent messages
		{"$match": map[string]interface{}{
			"user_id":    userID,
			"created_at": map[string]interface{}{"$gte": cutoff},
		}},
		// Stage 2: Group by hour of day
		{"$group": map[string]interface{}{
			"_id":            map[string]interface{}{"$hour": "$created_at"},
			"message_count":  map[string]interface{}{"$sum": 1},
			"unique_guilds":  map[string]interface{}{"$addToSet": "$guild_id"},
			"avg_length":     map[string]interface{}{"$avg": map[string]interface{}{"$strLenCP": "$content"}},
		}},
		// Stage 3: Add hour formatting
		{"$addFields": map[string]interface{}{
			"hour": "$_id",
			"hour_formatted": map[string]interface{}{
				"$concat": []interface{}{
					map[string]interface{}{"$cond": []interface{}{map[string]interface{}{"$lt": []interface{}{"$_id", 10}}, "0", ""}},
					map[string]interface{}{"$toString": "$_id"},
					":00",
				},
			},
			"guild_count": map[string]interface{}{"$size": "$unique_guilds"},
		}},
		// Stage 4: Sort by hour
		{"$sort": map[string]interface{}{"hour": 1}},
		// Stage 5: Project final format
		{"$project": map[string]interface{}{
			"hour":       "$hour_formatted",
			"messages":   "$message_count",
			"guilds":     "$guild_count",
			"avg_length": map[string]interface{}{"$round": []interface{}{"$avg_length", 1}},
			"_id":        0,
		}},
	}
}

// UserMentionAnalysis returns an optimized pipeline for analyzing who mentions the user most
func (ao *AggregationOptimizer) UserMentionAnalysis(userID int64, days int) []map[string]interface{} {
	cutoff := time.Now().UTC().AddDate(0, 0, -days)

	return []map[string]interface{}{
		// Stage 1: Match mentions of the user
		{"$match": map[string]interface{}{
			"target_id":  userID,
			"created_at": map[string]interface{}{"$gte": cutoff},
		}},
		// Stage 2: Group by mentioner
		{"$group": map[string]interface{}{
			"_id":             "$mentioner_id",
			"mention_count":   map[string]interface{}{"$sum": 1},
			"unique_guilds":   map[string]interface{}{"$addToSet": "$guild_id"},
			"unique_channels": map[string]interface{}{"$addToSet": "$channel_id"},
			"first_mention":   map[string]interface{}{"$min": "$created_at"},
			"last_mention":    map[string]interface{}{"$max": "$created_at"},
		}},
		// Stage 3: Add computed fields
		{"$addFields": map[string]interface{}{
			"guild_count":   map[string]interface{}{"$size": "$unique_guilds"},
			"channel_count": map[string]interface{}{"$size": "$unique_channels"},
		}},
		// Stage 4: Sort by mention count
		{"$sort": map[string]interface{}{"mention_count": -1}},
		// Stage 5: Limit to top 20
		{"$limit": 20},
		// Stage 6: Lookup mentioner details
		{"$lookup": map[string]interface{}{
			"from":         "users",
			"localField":   "_id",
			"foreignField": "user_id",
			"as":           "mentioner_info",
			"pipeline": []map[string]interface{}{
				{"$project": map[string]interface{}{
					"current_username":    1,
					"current_displayname": 1,
				}},
			},
		}},
		// Stage 7: Project final format
		{"$project": map[string]interface{}{
			"mentioner_id":  "$_id",
			"username":      map[string]interface{}{"$arrayElemAt": []interface{}{"$mentioner_info.current_username", 0}},
			"displayname":   map[string]interface{}{"$arrayElemAt": []interface{}{"$mentioner_info.current_displayname", 0}},
			"mentions":      "$mention_count",
			"guilds":        "$guild_count",
			"channels":      "$channel_count",
			"first_mention": "$first_mention",
			"last_mention":  "$last_mention",
			"_id":           0,
		}},
	}
}

// DeletedMessagesAnalysis returns an optimized pipeline for analyzing deleted message patterns
func (ao *AggregationOptimizer) DeletedMessagesAnalysis(days int, limit int) []map[string]interface{} {
	cutoff := time.Now().UTC().AddDate(0, 0, -days)

	return []map[string]interface{}{
		// Stage 1: Match recent deletions
		{"$match": map[string]interface{}{
			"deleted_at": map[string]interface{}{"$gte": cutoff},
		}},
		// Stage 2: Group by user
		{"$group": map[string]interface{}{
			"_id":                "$user_id",
			"deleted_count":      map[string]interface{}{"$sum": 1},
			"unique_guilds":      map[string]interface{}{"$addToSet": "$guild_id"},
			"unique_channels":    map[string]interface{}{"$addToSet": "$channel_id"},
			"avg_content_length": map[string]interface{}{"$avg": map[string]interface{}{"$strLenCP": "$content"}},
			"deletion_times":     map[string]interface{}{"$push": "$deleted_at"},
		}},
		// Stage 3: Add computed metrics
		{"$addFields": map[string]interface{}{
			"guild_count":   map[string]interface{}{"$size": "$unique_guilds"},
			"channel_count": map[string]interface{}{"$size": "$unique_channels"},
			"avg_length":    map[string]interface{}{"$round": []interface{}{"$avg_content_length", 1}},
		}},
		// Stage 4: Filter users with significant deletions
		{"$match": map[string]interface{}{
			"deleted_count": map[string]interface{}{"$gte": 3},
		}},
		// Stage 5: Sort by deletion count
		{"$sort": map[string]interface{}{"deleted_count": -1}},
		// Stage 6: Limit results
		{"$limit": limit},
		// Stage 7: Lookup user details
		{"$lookup": map[string]interface{}{
			"from":         "users",
			"localField":   "_id",
			"foreignField": "user_id",
			"as":           "user_info",
			"pipeline": []map[string]interface{}{
				{"$project": map[string]interface{}{
					"current_username":    1,
					"current_displayname": 1,
				}},
			},
		}},
		// Stage 8: Project final format
		{"$project": map[string]interface{}{
			"user_id":            "$_id",
			"username":           map[string]interface{}{"$arrayElemAt": []interface{}{"$user_info.current_username", 0}},
			"displayname":        map[string]interface{}{"$arrayElemAt": []interface{}{"$user_info.current_displayname", 0}},
			"deletions":          "$deleted_count",
			"guilds":             "$guild_count",
			"channels":           "$channel_count",
			"avg_message_length": "$avg_length",
			"_id":                0,
		}},
	}
}

// GuildActivityOverview returns an optimized pipeline for guild activity with channel breakdowns
func (ao *AggregationOptimizer) GuildActivityOverview(guildID int64, days int) []map[string]interface{} {
	cutoff := time.Now().UTC().AddDate(0, 0, -days)

	return []map[string]interface{}{
		// Stage 1: Match guild and recent messages
		{"$match": map[string]interface{}{
			"guild_id":   guildID,
			"created_at": map[string]interface{}{"$gte": cutoff},
		}},
		// Stage 2: Group by channel
		{"$group": map[string]interface{}{
			"_id":           "$channel_id",
			"message_count": map[string]interface{}{"$sum": 1},
			"unique_users":  map[string]interface{}{"$addToSet": "$user_id"},
			"total_chars":   map[string]interface{}{"$sum": map[string]interface{}{"$strLenCP": "$content"}},
			"first_message": map[string]interface{}{"$min": "$created_at"},
			"last_message":  map[string]interface{}{"$max": "$created_at"},
		}},
		// Stage 3: Add computed metrics
		{"$addFields": map[string]interface{}{
			"user_count":         map[string]interface{}{"$size": "$unique_users"},
			"avg_message_length": map[string]interface{}{"$divide": []interface{}{"$total_chars", "$message_count"}},
			"activity_span_days": map[string]interface{}{
				"$divide": []interface{}{
					map[string]interface{}{"$subtract": []interface{}{"$last_message", "$first_message"}},
					86400000,
				},
			},
		}},
		// Stage 4: Sort by message count
		{"$sort": map[string]interface{}{"message_count": -1}},
		// Stage 5: Add ranking
		{"$group": map[string]interface{}{
			"_id":      nil,
			"channels": map[string]interface{}{"$push": "$$ROOT"},
		}},
		// Stage 6: Unwind with index for ranking
		{"$unwind": map[string]interface{}{
			"path":              "$channels",
			"includeArrayIndex": "rank",
		}},
		// Stage 7: Project final format with rank
		{"$project": map[string]interface{}{
			"channel_id":    "$channels._id",
			"rank":          map[string]interface{}{"$add": []interface{}{"$rank", 1}},
			"messages":      "$channels.message_count",
			"users":         "$channels.user_count",
			"avg_length":    map[string]interface{}{"$round": []interface{}{"$channels.avg_message_length", 1}},
			"activity_days": map[string]interface{}{"$round": []interface{}{"$channels.activity_span_days", 1}},
			"first_message": "$channels.first_message",
			"last_message":  "$channels.last_message",
			"_id":           0,
		}},
	}
}

// UserGrowthTrend returns an optimized pipeline for tracking new user appearances
func (ao *AggregationOptimizer) UserGrowthTrend(days int) []map[string]interface{} {
	cutoff := time.Now().UTC().AddDate(0, 0, -days)

	return []map[string]interface{}{
		// Stage 1: Match users first seen in the period
		{"$match": map[string]interface{}{
			"first_seen": map[string]interface{}{"$gte": cutoff},
		}},
		// Stage 2: Group by day
		{"$group": map[string]interface{}{
			"_id": map[string]interface{}{
				"$dateToString": map[string]interface{}{
					"format": "%Y-%m-%d",
					"date":   "$first_seen",
				},
			},
			"new_users": map[string]interface{}{"$sum": 1},
			"user_ids":  map[string]interface{}{"$push": "$user_id"},
		}},
		// Stage 3: Sort by date
		{"$sort": map[string]interface{}{"_id": 1}},
		// Stage 4: Add cumulative count
		{"$group": map[string]interface{}{
			"_id":        nil,
			"daily_data": map[string]interface{}{"$push": "$$ROOT"},
		}},
		// Stage 5: Calculate running total
		{"$project": map[string]interface{}{
			"daily_data": map[string]interface{}{
				"$map": map[string]interface{}{
					"input": map[string]interface{}{"$range": []interface{}{0, map[string]interface{}{"$size": "$daily_data"}}},
					"in": map[string]interface{}{
						"date":      map[string]interface{}{"$arrayElemAt": []interface{}{"$daily_data._id", "$$this"}},
						"new_users": map[string]interface{}{"$arrayElemAt": []interface{}{"$daily_data.new_users", "$$this"}},
						"cumulative": map[string]interface{}{
							"$sum": map[string]interface{}{
								"$slice": []interface{}{
									"$daily_data.new_users",
									0,
									map[string]interface{}{"$add": []interface{}{"$$this", 1}},
								},
							},
						},
					},
				},
			},
		}},
		// Stage 6: Unwind for final output
		{"$unwind": "$daily_data"},
		// Stage 7: Project final format
		{"$project": map[string]interface{}{
			"date":        "$daily_data.date",
			"new_users":   "$daily_data.new_users",
			"total_users": "$daily_data.cumulative",
			"_id":         0,
		}},
	}
}

// ExecuteOptimizedAggregation executes a pipeline with optional index hints
func (ao *AggregationOptimizer) ExecuteOptimizedAggregation(collection string, pipeline []map[string]interface{}, hintIndex map[string]interface{}) ([]map[string]interface{}, error) {
	result, err := GlobalAggregate(collection, pipeline)
	if err != nil {
		log.Printf("[AggregationOptimizer] Pipeline execution failed on %s: %v", collection, err)
		return nil, err
	}
	log.Printf("[AggregationOptimizer] Executed pipeline on %s: %d results", collection, len(result))
	return result, nil
}

// CreateTextSearchPipeline returns an optimized text search pipeline with scoring
func (ao *AggregationOptimizer) CreateTextSearchPipeline(collection string, searchTerm string, limit int) []map[string]interface{} {
	return []map[string]interface{}{
		// Stage 1: Text search with index
		{"$match": map[string]interface{}{
			"$text": map[string]interface{}{
				"$search":        searchTerm,
				"$caseSensitive": false,
			},
		}},
		// Stage 2: Add search score
		{"$addFields": map[string]interface{}{
			"search_score": map[string]interface{}{"$meta": "textScore"},
		}},
		// Stage 3: Sort by relevance
		{"$sort": map[string]interface{}{
			"search_score": -1,
			"created_at":   -1,
		}},
		// Stage 4: Limit results
		{"$limit": limit},
		// Stage 5: Remove internal score field
		{"$project": map[string]interface{}{
			"search_score": 0,
		}},
	}
}

// Global aggregation optimizer instance
var _aggOptimizer *AggregationOptimizer

func GetAggregationOptimizer(dbManager *DatabaseManager) *AggregationOptimizer {
	if _aggOptimizer == nil {
		_aggOptimizer = NewAggregationOptimizer(dbManager)
	}
	return _aggOptimizer
}