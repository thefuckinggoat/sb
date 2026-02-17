package database

import (
	"fmt"
	"log"
)

// MonitorDBStatus monitors the shared database connection status
func MonitorDBStatus() {
	db := GetGlobalDB()
	if db == nil {
		fmt.Println("❌ Database Connection Error: Global database not initialized")
		return
	}

	if !IsGlobalDBActive() {
		fmt.Println("❌ Database Connection Error: Global database not active")
		return
	}

	fmt.Println("✅ Shared Database Connection Status:")
	fmt.Printf("   - Is Active: %v\n", IsGlobalDBActive())
	fmt.Printf("   - Database: %s\n", db.Name())

	// Test a simple operation
	result, err := GlobalFindOne("users", map[string]interface{}{}, map[string]interface{}{"_id": 1})
	if err != nil {
		fmt.Printf("   - Test Query: ❌ Error: %v\n", err)
	} else if result != nil {
		fmt.Println("   - Test Query: ✅ Success")
	} else {
		fmt.Println("   - Test Query: ⚠️  No users found")
	}
}

// RunMonitor is the entry point if run standalone
func RunMonitor() {
	if err := InitializeGlobalDatabase(); err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer ShutdownGlobalDatabase()

	MonitorDBStatus()
}