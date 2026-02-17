package utils

import (
	"encoding/json"
	"log"
	"os"

	"lostselfbot/utils/database"
)

// MigrateConfigs migrates config.json user configs to database
func MigrateConfigs() error {
	if err := database.InitializeGlobalDatabase(); err != nil {
		return err
	}
	defer database.ShutdownGlobalDatabase()

	data, err := os.ReadFile("config.json")
	if err != nil {
		return err
	}

	var config map[string]interface{}
	if err := json.Unmarshal(data, &config); err != nil {
		return err
	}

	users, ok := config["users"].([]interface{})
	if !ok {
		log.Println("[migrate] No users array found in config")
		return nil
	}

	for _, u := range users {
		userMap, ok := u.(map[string]interface{})
		if !ok {
			continue
		}

		userIDRaw, ok := userMap["user_id"]
		if !ok {
			continue
		}

		var userID int64
		switch v := userIDRaw.(type) {
		case float64:
			userID = int64(v)
		case string:
			// skip if not numeric
			continue
		default:
			continue
		}

		if userID == 0 {
			continue
		}

		// Check if already exists
		existing, err := database.GlobalFindOne("user_configs", map[string]interface{}{"_id": userID}, nil)
		if err != nil {
			log.Printf("[migrate] Error checking existing config for %d: %v", userID, err)
			continue
		}

		if existing != nil {
			continue
		}

		prefix := "-"
		if p, ok := config["command_prefix"].(string); ok {
			prefix = p
		}

		newConfig := map[string]interface{}{
			"_id":            userID,
			"user_id":        userID,
			"command_prefix":  prefix,
			"auto_delete":     map[string]interface{}{"enabled": true, "delay": 5},
			"presence":        map[string]interface{}{},
		}

		if err := database.GlobalInsertOne("user_configs", newConfig); err != nil {
			log.Printf("[migrate] Error inserting config for %d: %v", userID, err)
			continue
		}

		log.Printf("[migrate] Migrated config for user %d", userID)
	}

	return nil
}