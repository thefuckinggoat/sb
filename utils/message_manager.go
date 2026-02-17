package utils

import (
	"fmt"
	"log"
	"time"
)

// MessageRef holds a reference to a message for deletion
type MessageRef struct {
	ChannelID string
	MessageID string
}

// MessageManager handles message management and cleanup
type MessageManager struct {
	deleteFunc func(channelID, messageID string) error
}

func NewMessageManager(deleteFunc func(channelID, messageID string) error) *MessageManager {
	return &MessageManager{
		deleteFunc: deleteFunc,
	}
}

// ScheduleDeletion schedules messages for deletion after a delay
func (mm *MessageManager) ScheduleDeletion(messages []MessageRef, delay time.Duration) {
	if len(messages) == 0 {
		return
	}

	go func() {
		time.Sleep(delay)
		for _, msg := range messages {
			if err := mm.deleteFunc(msg.ChannelID, msg.MessageID); err != nil {
				log.Printf("[MessageManager] Error deleting message %s: %v", msg.MessageID, err)
			}
		}
	}()
}

// FormatDeletionError formats a deletion error message
func FormatDeletionError(messageID string, err error) string {
	return fmt.Sprintf("Error deleting message %s: %v", messageID, err)
}