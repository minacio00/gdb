package main

import (
	"fmt"
	"log"

	"github.com/minacio00/gdb/storageengine"
)

func main() {
	db, err := storageengine.NewDatabase("test.db", 4096)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tables := db.ListTables()
	fmt.Println("Existing tables:", tables)

	if len(tables) == 0 || !contains(tables, "users") {
		columns := []storageengine.Column{
			{Name: "id", Type: storageengine.TInteger, NotNull: true},
			{Name: "name", Type: storageengine.Tstring, NotNull: true},
			{Name: "age", Type: storageengine.TInteger, NotNull: false},
			{Name: "is_active", Type: storageengine.Tbool, NotNull: true},
			{Name: "salary", Type: storageengine.Tfloat, NotNull: false},
		}

		err = db.CreateTable("users", columns, "id")
		if err != nil {
			log.Fatalf("Failed to create table: %v", err)
		}
		fmt.Println("Created 'users' table")
	}

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
			log.Printf("Failed to insert user: %v", err)
		} else {
			fmt.Printf("Inserted user: %s\n", user["name"])
		}
	}

	count, err := db.GetRowCount("users")
	if err != nil {
		log.Printf("Error getting row count: %v", err)
	} else {
		fmt.Printf("Total users: %d\n", count)
	}

	fmt.Println("\nAll Users:")
	allUsers, err := db.SelectAll("users")
	if err != nil {
		log.Printf("Error selecting all users: %v", err)
	} else {
		printRows(allUsers)
	}

	fmt.Println("\nActive Users:")
	activeUsers, err := db.Select("users", func(row *storageengine.Row) bool {
		active, ok := row.Values["is_active"].(bool)
		return ok && active
	})
	if err != nil {
		log.Printf("Error selecting active users: %v", err)
	} else {
		printRows(activeUsers)
	}

	fmt.Println("\nUser with ID 2:")
	user, err := db.SelectByID("users", 2)
	if err != nil {
		log.Printf("Error selecting user by ID: %v", err)
	} else {
		printRow(user)
	}

	fmt.Println("\nUsers over 30:")
	olderUsers, err := db.Select("users", func(row *storageengine.Row) bool {
		age, ok := row.Values["age"].(int64)
		return ok && age > 30
	})
	if err != nil {
		log.Printf("Error selecting older users: %v", err)
	} else {
		printRows(olderUsers)
	}
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func printRows(rows []*storageengine.Row) {
	for _, row := range rows {
		printRow(row)
	}
}

func printRow(row *storageengine.Row) {
	fmt.Printf("Row ID: %d\n", row.RowID)
	for k, v := range row.Values {
		fmt.Printf("  %s: %v\n", k, v)
	}
	fmt.Println()
}
