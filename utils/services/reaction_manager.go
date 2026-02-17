package services

import (
	"sync"
)

// ReactionManager manages self-reactions, user-reactions, rotating reactions, and super reactions
type ReactionManager struct {
	mu                sync.RWMutex
	selfReactions     []string
	userReactions     map[string][]string            // userID -> []emoji
	rotatingReactions map[string][][]string           // userID -> [][]emoji (groups)
	superReactions    map[string]map[string]bool      // userID -> emoji -> isSuperReaction
}

func NewReactionManager() *ReactionManager {
	return &ReactionManager{
		selfReactions:     make([]string, 0),
		userReactions:     make(map[string][]string),
		rotatingReactions: make(map[string][][]string),
		superReactions:    make(map[string]map[string]bool),
	}
}

// ─── Self Reactions ───

func (rm *ReactionManager) SetSelfReactions(reactions []string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.selfReactions = make([]string, len(reactions))
	copy(rm.selfReactions, reactions)
}

func (rm *ReactionManager) GetSelfReactions() []string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	result := make([]string, len(rm.selfReactions))
	copy(result, rm.selfReactions)
	return result
}

func (rm *ReactionManager) HasSelfReactions() bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return len(rm.selfReactions) > 0
}

func (rm *ReactionManager) ClearSelfReactions() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.selfReactions = make([]string, 0)
}

// ─── User Reactions ───

func (rm *ReactionManager) SetUserReactions(userID string, reactions []string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	cp := make([]string, len(reactions))
	copy(cp, reactions)
	rm.userReactions[userID] = cp
}

func (rm *ReactionManager) GetUserReactions(userID string) []string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	reactions, ok := rm.userReactions[userID]
	if !ok {
		return nil
	}
	result := make([]string, len(reactions))
	copy(result, reactions)
	return result
}

func (rm *ReactionManager) HasUserReactions(userID string) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	reactions, ok := rm.userReactions[userID]
	return ok && len(reactions) > 0
}

func (rm *ReactionManager) ClearUserReactions(userID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	delete(rm.userReactions, userID)
}

// ─── Rotating Reactions ───

func (rm *ReactionManager) SetRotatingReactions(userID string, reactions [][]string, superMode bool) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Deep copy the groups
	processed := make([][]string, 0, len(reactions))
	for _, group := range reactions {
		if len(group) > 0 {
			cp := make([]string, len(group))
			copy(cp, group)
			processed = append(processed, cp)
		}
	}

	rm.rotatingReactions[userID] = processed

	// If super mode, mark all emojis in all groups as super reactions
	if superMode {
		if _, ok := rm.superReactions[userID]; !ok {
			rm.superReactions[userID] = make(map[string]bool)
		}
		for _, group := range processed {
			for _, emoji := range group {
				rm.superReactions[userID][emoji] = true
			}
		}
	}
}

func (rm *ReactionManager) GetRotatingReactions(userID string) [][]string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	groups, ok := rm.rotatingReactions[userID]
	if !ok {
		return nil
	}
	// Deep copy
	result := make([][]string, len(groups))
	for i, group := range groups {
		cp := make([]string, len(group))
		copy(cp, group)
		result[i] = cp
	}
	return result
}

func (rm *ReactionManager) HasRotatingReactions(userID string) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	groups, ok := rm.rotatingReactions[userID]
	return ok && len(groups) > 0
}

func (rm *ReactionManager) ClearRotatingReactions(userID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	delete(rm.rotatingReactions, userID)
}

// ─── Super Reactions ───

func (rm *ReactionManager) SetSuperReaction(userID string, emoji string, isSuper bool) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if _, ok := rm.superReactions[userID]; !ok {
		rm.superReactions[userID] = make(map[string]bool)
	}
	rm.superReactions[userID][emoji] = isSuper
}

func (rm *ReactionManager) IsSuperReaction(userID string, emoji string) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	userSuper, ok := rm.superReactions[userID]
	if !ok {
		return false
	}
	return userSuper[emoji]
}

func (rm *ReactionManager) GetSuperReactions(userID string) map[string]bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	userSuper, ok := rm.superReactions[userID]
	if !ok {
		return nil
	}
	result := make(map[string]bool)
	for emoji, isSuper := range userSuper {
		if isSuper {
			result[emoji] = true
		}
	}
	return result
}

func (rm *ReactionManager) ClearSuperReactions(userID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	delete(rm.superReactions, userID)
}