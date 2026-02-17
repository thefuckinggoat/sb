package utils

import (
	"fmt"
	"strings"
)

// AddEmojisToUser adds emojis to a user's auto-react list
func AddEmojisToUser(userEmojis map[interface{}][]string, userID interface{}, emojis []string) string {
	if _, ok := userEmojis[userID]; !ok {
		userEmojis[userID] = []string{}
	}
	userEmojis[userID] = append(userEmojis[userID], emojis...)
	return fmt.Sprintf("Emojis %s added for user %v", strings.Join(emojis, ", "), userID)
}