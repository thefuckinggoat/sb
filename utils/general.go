package utils

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

// MAX_MESSAGE_LENGTH stores per-user message limits
var (
	maxMessageLength   = make(map[string]int)
	maxMessageLengthMu sync.RWMutex
)

// DetectMessageLimit detects max message length for a user (nitro vs non-nitro)
func DetectMessageLimit(userID string, hasNitro bool) int {
	maxMessageLengthMu.Lock()
	defer maxMessageLengthMu.Unlock()

	if limit, ok := maxMessageLength[userID]; ok {
		return limit
	}

	limit := 2000
	if hasNitro {
		limit = 4000
	}
	maxMessageLength[userID] = limit
	log.Printf("[util] Message limit for %s: %d (Nitro: %v)", userID, limit, hasNitro)
	return limit
}

// GetMaxMessageLength returns the cached max message length
func GetMaxMessageLength(userID string) int {
	maxMessageLengthMu.RLock()
	defer maxMessageLengthMu.RUnlock()

	if limit, ok := maxMessageLength[userID]; ok {
		return limit
	}
	return 2000
}

// FormatAsYAMLCodeBlock formats results as a YAML code block
func FormatAsYAMLCodeBlock(results []string) string {
	var formatted []string
	for _, result := range results {
		formatted = append(formatted, result)
	}
	yamlFormatted := strings.Join(formatted, "\n\n")
	return fmt.Sprintf("```yaml\n%s\n```", yamlFormatted)
}

// CalculateChunkSize determines the optimal chunk size for paginated results
func CalculateChunkSize(totalResults int) int {
	if totalResults <= 9 {
		return totalResults
	} else if totalResults <= 18 {
		return (totalResults + 1) / 2
	}
	size := (totalResults + 2) / 3
	if size < 9 {
		size = 9
	}
	if size > 20 {
		size = 20
	}
	return size
}

// IsValidEmoji checks if a string is a valid emoji
func IsValidEmoji(emoji string) bool {
	// Reject colon-wrapped short text like :sk:
	if strings.HasPrefix(emoji, ":") && strings.HasSuffix(emoji, ":") && len(emoji) > 2 {
		name := emoji[1 : len(emoji)-1]
		if len(name) <= 2 || isAllAlpha(name) {
			return false
		}
	}

	// Check for custom Discord emoji format <:name:id> or <a:name:id>
	if strings.HasPrefix(emoji, "<") && strings.HasSuffix(emoji, ">") {
		inner := emoji[1 : len(emoji)-1]
		if strings.HasPrefix(inner, ":") || strings.HasPrefix(inner, "a:") {
			parts := strings.Split(inner, ":")
			if len(parts) >= 3 {
				_, err := strconv.ParseInt(parts[len(parts)-1], 10, 64)
				return err == nil
			}
		}
		return false
	}

	// Reject pure alphabetic strings longer than 1 char
	if isAllAlpha(emoji) && utf8.RuneCountInString(emoji) > 1 {
		return false
	}

	// Check if it's a Unicode emoji (has non-ASCII runes)
	for _, r := range emoji {
		if r > 127 && !unicode.IsLetter(r) {
			return true
		}
	}

	// Single character non-ASCII
	if utf8.RuneCountInString(emoji) == 1 {
		r, _ := utf8.DecodeRuneInString(emoji)
		return r > 127
	}

	return false
}

func isAllAlpha(s string) bool {
	for _, r := range s {
		if !unicode.IsLetter(r) {
			return false
		}
	}
	return len(s) > 0
}

// FilterValidEmojis filters a list to only valid emojis
func FilterValidEmojis(emojis []string) []string {
	var valid []string
	for _, emoji := range emojis {
		if IsValidEmoji(emoji) {
			valid = append(valid, emoji)
		}
	}
	return valid
}

// FormatMessage formats a message with optional code block wrapping
func FormatMessage(content string, codeBlock bool, escapeBackticks bool) string {
	if escapeBackticks && codeBlock {
		content = strings.ReplaceAll(content, "`", "\\`")
	}

	if codeBlock {
		if strings.Contains(content, "`") && !escapeBackticks {
			return content
		}
		return fmt.Sprintf("`%s`", content)
	}

	return content
}

// QuoteBlock adds > prefix to each line
func QuoteBlock(text string) string {
	lines := strings.Split(text, "\n")
	var quoted []string
	for _, line := range lines {
		quoted = append(quoted, fmt.Sprintf("> %s", line))
	}
	return strings.Join(quoted, "\n")
}

// ParseUsersAndEmojis parses a list of args into user IDs and emojis
func ParseUsersAndEmojis(args []string) ([]string, []string) {
	var users []string
	var emojis []string

	for _, arg := range args {
		if IsValidEmoji(arg) {
			emojis = append(emojis, arg)
		} else if strings.HasPrefix(arg, "<@") && strings.HasSuffix(arg, ">") {
			// Extract user ID from mention
			inner := arg[2 : len(arg)-1]
			inner = strings.TrimPrefix(inner, "!")
			if _, err := strconv.ParseInt(inner, 10, 64); err == nil {
				users = append(users, inner)
			}
		} else if _, err := strconv.ParseInt(arg, 10, 64); err == nil {
			// It's a user ID
			users = append(users, arg)
		} else {
			// Treat as username
			users = append(users, arg)
		}
	}

	return users, emojis
}