package utils

import (
	"time"
)

// UserSchema creates a user document from user data
func UserSchema(userData map[string]interface{}) map[string]interface{} {
	now := time.Now().UTC().Truncate(time.Second)

	doc := map[string]interface{}{
		"_id":                 userData["_id"],
		"name":                userData["name"],
		"current_username":    userData["current_username"],
		"current_displayname": userData["current_displayname"],
		"current_avatar_url":  userData["current_avatar_url"],
		"current_banner_url":  userData["current_banner_url"],
		"first_seen":          getOrDefault(userData, "first_seen", now),
		"last_seen":           getOrDefault(userData, "last_seen", now),
		"username_history":    getOrDefault(userData, "username_history", []interface{}{}),
		"displayname_history": getOrDefault(userData, "displayname_history", []interface{}{}),
		"avatar_history":      getOrDefault(userData, "avatar_history", []interface{}{}),
		"banner_history":      getOrDefault(userData, "banner_history", []interface{}{}),
	}
	return doc
}

// DeletedMessageSchema creates a deleted message document
func DeletedMessageSchema(messageData map[string]interface{}) map[string]interface{} {
	now := time.Now().UTC().Truncate(time.Second)

	return map[string]interface{}{
		"_id":          messageData["message_id"],
		"user_id":      messageData["user_id"],
		"channel_id":   messageData["channel_id"],
		"guild_id":     messageData["guild_id"],
		"content":      getOrDefault(messageData, "content", ""),
		"attachments":  getOrDefault(messageData, "attachments", []interface{}{}),
		"deleted_at":   getOrDefault(messageData, "deleted_at", now),
		"channel_name": getOrDefault(messageData, "channel_name", "Unknown"),
		"guild_name":   messageData["guild_name"],
		"author_name":  getOrDefault(messageData, "author_name", "Unknown"),
	}
}

// HistoryEntrySchema creates a history entry document
func HistoryEntrySchema(entryData map[string]interface{}) map[string]interface{} {
	now := time.Now().UTC().Truncate(time.Second)
	return map[string]interface{}{
		"value":      entryData["value"],
		"changed_at": getOrDefault(entryData, "changed_at", now),
	}
}

// ValidateUserData validates user data has required fields
func ValidateUserData(userData map[string]interface{}) bool {
	required := []string{"_id", "name", "current_username"}
	for _, field := range required {
		if _, ok := userData[field]; !ok {
			return false
		}
	}
	return true
}

// ValidateDeletedMessage validates deleted message data has required fields
func ValidateDeletedMessage(messageData map[string]interface{}) bool {
	required := []string{"message_id", "user_id", "channel_id"}
	for _, field := range required {
		if _, ok := messageData[field]; !ok {
			return false
		}
	}
	return true
}

func getOrDefault(data map[string]interface{}, key string, defaultVal interface{}) interface{} {
	if val, ok := data[key]; ok && val != nil {
		return val
	}
	return defaultVal
}