package storageengine

import (
	"os"
	"testing"
)

// TestDatabaseOperations tests the basic database operations
func TestDatabaseOperations(t *testing.T) {
	// Create a temporary database file
	dbPath := "test_db.db"
	defer os.Remove(dbPath) // Clean up after test

	// Create a new database
	db, err := NewDatabase(dbPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Test CreateTable
	columns := []Column{
		{Name: "id", Type: TInteger, NotNull: true},
		{Name: "name", Type: Tstring, NotNull: true},
		{Name: "age", Type: TInteger, NotNull: false},
		{Name: "is_active", Type: Tbool, NotNull: true},
		{Name: "salary", Type: Tfloat, NotNull: false},
	}

	err = db.CreateTable("users", columns, "id")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Verify table was created
	tables := db.ListTables()
	if len(tables) != 1 || tables[0] != "users" {
		t.Fatalf("Expected table 'users' to be created, got %v", tables)
	}

	// Test Insert
	users := []map[string]interface{}{
		{
			"id":        int64(1),
			"name":      "John Doe",
			"age":       int64(30),
			"is_active": true,
			"salary":    float64(75000.50),
		},
		{
			"id":        int64(2),
			"name":      "Jane Smith",
			"age":       int64(25),
			"is_active": true,
			"salary":    float64(82000.75),
		},
		{
			"id":        int64(3),
			"name":      "Bob Johnson",
			"age":       int64(40),
			"is_active": false,
			"salary":    nil, // NULL value for salary
		},
	}

	for _, user := range users {
		err = db.Insert("users", user)
		if err != nil {
			t.Fatalf("Failed to insert user: %v", err)
		}
	}

	// Verify row count
	count, err := db.GetRowCount("users")
	if err != nil {
		t.Fatalf("Failed to get row count: %v", err)
	}
	if count != 3 {
		t.Fatalf("Expected 3 rows, got %d", count)
	}

	// Test SelectAll
	allUsers, err := db.SelectAll("users")
	if err != nil {
		t.Fatalf("Failed to select all users: %v", err)
	}
	if len(allUsers) != 3 {
		t.Fatalf("Expected 3 users, got %d", len(allUsers))
	}

	// Test Select with condition
	activeUsers, err := db.Select("users", func(row *Row) bool {
		active, ok := row.Values["is_active"].(bool)
		return ok && active
	})
	if err != nil {
		t.Fatalf("Failed to select active users: %v", err)
	}
	if len(activeUsers) != 2 {
		t.Fatalf("Expected 2 active users, got %d", len(activeUsers))
	}

	// Test SelectByID
	user, err := db.SelectByID("users", 2)
	if err != nil {
		t.Fatalf("Failed to select user by ID: %v", err)
	}
	if user.Values["name"] != "Jane Smith" {
		t.Fatalf("Expected user name 'Jane Smith', got '%v'", user.Values["name"])
	}

	// Test SelectWhere
	olderUsers, err := db.SelectWhere("users", "age", ">", int64(30))
	if err != nil {
		t.Fatalf("Failed to select older users: %v", err)
	}
	if len(olderUsers) != 1 {
		t.Fatalf("Expected 1 older user, got %d", len(olderUsers))
	}
	if olderUsers[0].Values["name"] != "Bob Johnson" {
		t.Fatalf("Expected older user to be 'Bob Johnson', got '%v'", olderUsers[0].Values["name"])
	}

	// Test SelectWhere with string comparison
	johnUsers, err := db.SelectWhere("users", "name", "=", "John Doe")
	if err != nil {
		t.Fatalf("Failed to select users named John: %v", err)
	}
	if len(johnUsers) != 1 {
		t.Fatalf("Expected 1 user named John, got %d", len(johnUsers))
	}

	// Test SelectWhere with salary comparison
	highPaidUsers, err := db.SelectWhere("users", "salary", ">=", float64(70000.0))
	if err != nil {
		t.Fatalf("Failed to select high paid users: %v", err)
	}
	if len(highPaidUsers) != 2 {
		t.Fatalf("Expected 2 high paid users, got %d", len(highPaidUsers))
	}
}

// TestDatabasePersistence tests that data persists across database sessions
func TestDatabasePersistence(t *testing.T) {
	dbPath := "persistence_test.db"
	defer os.Remove(dbPath) // Clean up after test

	// Create and populate database
	{
		db, err := NewDatabase(dbPath, 4096)
		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}

		columns := []Column{
			{Name: "id", Type: TInteger, NotNull: true},
			{Name: "name", Type: Tstring, NotNull: true},
		}

		err = db.CreateTable("test_table", columns, "id")
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		err = db.Insert("test_table", map[string]interface{}{
			"id":   int64(1),
			"name": "Test User",
		})
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}

		db.Close()
	}

	// Reopen database and verify data
	{
		db, err := NewDatabase(dbPath, 4096)
		if err != nil {
			t.Fatalf("Failed to reopen database: %v", err)
		}
		defer db.Close()

		tables := db.ListTables()
		if len(tables) != 1 || tables[0] != "test_table" {
			t.Fatalf("Expected table 'test_table' to be loaded, got %v", tables)
		}

		rows, err := db.SelectAll("test_table")
		if err != nil {
			t.Fatalf("Failed to select rows: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(rows))
		}
		if rows[0].Values["name"] != "Test User" {
			t.Fatalf("Expected user name 'Test User', got '%v'", rows[0].Values["name"])
		}
	}
}

// TestInvalidOperations tests that invalid operations are properly rejected
func TestInvalidOperations(t *testing.T) {
	dbPath := "invalid_test.db"
	defer os.Remove(dbPath) // Clean up after test

	db, err := NewDatabase(dbPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create a valid table first
	columns := []Column{
		{Name: "id", Type: TInteger, NotNull: true},
		{Name: "name", Type: Tstring, NotNull: true},
	}

	err = db.CreateTable("test_table", columns, "id")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test creating duplicate table
	err = db.CreateTable("test_table", columns, "id")
	if err == nil {
		t.Fatal("Expected error when creating duplicate table, got nil")
	}

	// Test inserting with invalid column type
	err = db.Insert("test_table", map[string]interface{}{
		"id":   "not an integer", // Wrong type for TInteger
		"name": "Test User",
	})
	if err == nil {
		t.Fatal("Expected error when inserting wrong type, got nil")
	}

	// Test inserting with missing required column
	err = db.Insert("test_table", map[string]interface{}{
		"name": "Test User",
		// Missing "id" which is NOT NULL
	})
	if err == nil {
		t.Fatal("Expected error when missing required column, got nil")
	}

	// Test inserting into non-existent table
	err = db.Insert("non_existent_table", map[string]interface{}{
		"id":   int64(1),
		"name": "Test User",
	})
	if err == nil {
		t.Fatal("Expected error when inserting into non-existent table, got nil")
	}

	// Test querying non-existent table
	_, err = db.SelectAll("non_existent_table")
	if err == nil {
		t.Fatal("Expected error when querying non-existent table, got nil")
	}
}
