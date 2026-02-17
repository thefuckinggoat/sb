package utils

// ReactToggleResult holds the result of toggling auto-react for a user
type ReactToggleResult struct {
	Identifier interface{} // string (username) or int64 (user ID)
	Enabled    bool
	Status     string // "enabled" or "disabled"
}

// ToggleAutoReact toggles auto-react for a list of user identifiers
func ToggleAutoReact(userAutoReact map[interface{}]bool, userEmojis map[interface{}][]string, userIdentifiers []interface{}, emojis []string) []ReactToggleResult {
	var results []ReactToggleResult

	for _, identifier := range userIdentifiers {
		if enabled, exists := userAutoReact[identifier]; exists && enabled {
			userAutoReact[identifier] = false
			delete(userEmojis, identifier)
			results = append(results, ReactToggleResult{
				Identifier: identifier,
				Enabled:    false,
				Status:     "disabled",
			})
		} else {
			userAutoReact[identifier] = true
			if len(emojis) > 0 {
				userEmojis[identifier] = emojis
			}
			results = append(results, ReactToggleResult{
				Identifier: identifier,
				Enabled:    true,
				Status:     "enabled",
			})
		}
	}

	return results
}