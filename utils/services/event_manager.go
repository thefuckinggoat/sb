package services

import (
	"encoding/json"
	"log"
	"sync"
)

// EventHandlerFunc is a function that handles a dispatched event
type EventHandlerFunc func(data json.RawMessage)

// EventManager is a centralized event management system
type EventManager struct {
	mu            sync.RWMutex
	eventHandlers map[string]map[string]EventHandlerFunc // eventName -> cogName -> handler
}

func NewEventManager() *EventManager {
	return &EventManager{
		eventHandlers: make(map[string]map[string]EventHandlerFunc),
	}
}

// RegisterHandler registers an event handler for a specific event and cog
func (em *EventManager) RegisterHandler(eventName string, cogName string, handler EventHandlerFunc) {
	em.mu.Lock()
	defer em.mu.Unlock()

	if _, ok := em.eventHandlers[eventName]; !ok {
		em.eventHandlers[eventName] = make(map[string]EventHandlerFunc)
	}
	em.eventHandlers[eventName][cogName] = handler
	log.Printf("[EventManager] Registered %s handler for %s", eventName, cogName)
}

// UnregisterHandler unregisters a specific event handler
func (em *EventManager) UnregisterHandler(eventName string, cogName string) {
	em.mu.Lock()
	defer em.mu.Unlock()

	if handlers, ok := em.eventHandlers[eventName]; ok {
		delete(handlers, cogName)
		log.Printf("[EventManager] Unregistered %s handler for %s", eventName, cogName)
	}
}

// UnregisterCog unregisters all handlers for a cog
func (em *EventManager) UnregisterCog(cogName string) {
	em.mu.Lock()
	defer em.mu.Unlock()

	for eventName, handlers := range em.eventHandlers {
		if _, ok := handlers[cogName]; ok {
			delete(handlers, cogName)
			log.Printf("[EventManager] Unregistered %s handler for %s", eventName, cogName)
		}
	}
	log.Printf("[EventManager] Unregistered all handlers for %s", cogName)
}

// DispatchEvent dispatches an event to all registered handlers
func (em *EventManager) DispatchEvent(eventName string, data json.RawMessage) {
	em.mu.RLock()
	handlers, ok := em.eventHandlers[eventName]
	if !ok {
		em.mu.RUnlock()
		return
	}

	// Copy handlers to avoid holding the lock during dispatch
	handlersCopy := make(map[string]EventHandlerFunc, len(handlers))
	for cogName, handler := range handlers {
		handlersCopy[cogName] = handler
	}
	em.mu.RUnlock()

	for cogName, handler := range handlersCopy {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[EventManager] Panic in %s handling %s: %v", cogName, eventName, r)
				}
			}()
			handler(data)
		}()
	}
}

// GetRegisteredEvents returns a list of all events that have handlers
func (em *EventManager) GetRegisteredEvents() []string {
	em.mu.RLock()
	defer em.mu.RUnlock()

	var events []string
	for eventName, handlers := range em.eventHandlers {
		if len(handlers) > 0 {
			events = append(events, eventName)
		}
	}
	return events
}

// GetHandlersForEvent returns cog names registered for a specific event
func (em *EventManager) GetHandlersForEvent(eventName string) []string {
	em.mu.RLock()
	defer em.mu.RUnlock()

	handlers, ok := em.eventHandlers[eventName]
	if !ok {
		return nil
	}

	var cogs []string
	for cogName := range handlers {
		cogs = append(cogs, cogName)
	}
	return cogs
}