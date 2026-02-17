package utils

import (
	"log"
)

// ErrorHandler provides centralized error handling for commands
type ErrorHandler struct{}

func NewErrorHandler() *ErrorHandler {
	return &ErrorHandler{}
}

// HandleCommandError handles errors from command execution
func (eh *ErrorHandler) HandleCommandError(command string, err error) string {
	if err == nil {
		return ""
	}

	errStr := err.Error()

	// Command not found — ignore
	if errStr == "command not found" {
		return ""
	}

	// Missing permissions
	if errStr == "missing permissions" {
		return "You don't have permission to use this command!"
	}

	// Bad argument
	if len(errStr) > 16 && errStr[:16] == "invalid argument" {
		return "Invalid argument: " + errStr[18:]
	}

	// Missing required argument
	if len(errStr) > 25 && errStr[:25] == "missing required argument" {
		return "Missing required argument: " + errStr[27:]
	}

	// Unexpected error
	log.Printf("[ErrorHandler] Unexpected error in command %s: %v", command, err)
	return "An unexpected error occurred. Please try again later."
}