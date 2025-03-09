package storageengine

import (
	"os"
	"testing"
)

// TestQueryOperations tests the various query operations
func TestQueryOperations(t *testing.T) {
	// Create a temporary database file
	dbPath := "query_test.db"
	defer os.Remove(dbPath) // Clean up after test

	// Create a new database
	db, err := NewDatabase(dbPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create products table
	columns := []Column{
		{Name: "id", Type: TInteger, NotNull: true},
		{Name: "name", Type: Tstring, NotNull: true},
		{Name: "price", Type: Tfloat, NotNull: true},
		{Name: "in_stock", Type: Tbool, NotNull: true},
		{Name: "category", Type: Tstring, NotNull: false},
	}

	err = db.CreateTable("products", columns, "id")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	products := []map[string]interface{}{
		{
			"id":       int64(1),
			"name":     "Laptop",
			"price":    float64(1200.99),
			"in_stock": true,
			"category": "Electronics",
		},
		{
			"id":       int64(2),
			"name":     "Desk Chair",
			"price":    float64(199.50),
			"in_stock": true,
			"category": "Furniture",
		},
		{
			"id":       int64(3),
			"name":     "Coffee Mug",
			"price":    float64(12.99),
			"in_stock": true,
			"category": "Kitchen",
		},
		{
			"id":       int64(4),
			"name":     "Gaming Console",
			"price":    float64(499.99),
			"in_stock": false,
			"category": "Electronics",
		},
		{
			"id":       int64(5),
			"name":     "Bookshelf",
			"price":    float64(149.00),
			"in_stock": true,
			"category": "Furniture",
		},
	}

	for _, product := range products {
		err = db.Insert("products", product)
		if err != nil {
			t.Fatalf("Failed to insert product: %v", err)
		}
	}

	// Test different query operations

	// Test: Select products by price range
	t.Run("PriceRangeQuery", func(t *testing.T) {
		// Select products between $100 and $500
		mediumPriceProducts, err := db.Select("products", func(row *Row) bool {
			price, ok := row.Values["price"].(float64)
			return ok && price >= 100.0 && price <= 500.0
		})

		if err != nil {
			t.Fatalf("Failed to execute price range query: %v", err)
		}

		if len(mediumPriceProducts) != 3 {
			t.Fatalf("Expected 3 medium price products, got %d", len(mediumPriceProducts))
		}
	})

	// Test: SelectWhere with ">=" operator
	t.Run("PriceComparisonQuery", func(t *testing.T) {
		// Select expensive products (>= $500)
		expensiveProducts, err := db.SelectWhere("products", "price", ">=", float64(500.0))

		if err != nil {
			t.Fatalf("Failed to execute expensive products query: %v", err)
		}

		if len(expensiveProducts) != 2 {
			t.Fatalf("Expected 2 expensive products, got %d", len(expensiveProducts))
		}
	})

	// Test: SelectWhere with "=" operator on string column
	t.Run("CategoryQuery", func(t *testing.T) {
		// Select electronics
		electronics, err := db.SelectWhere("products", "category", "=", "Electronics")

		if err != nil {
			t.Fatalf("Failed to execute category query: %v", err)
		}

		if len(electronics) != 2 {
			t.Fatalf("Expected 2 electronic products, got %d", len(electronics))
		}
	})

	// Test: SelectWhere with boolean column
	t.Run("InStockQuery", func(t *testing.T) {
		// Select in-stock products
		inStockProducts, err := db.SelectWhere("products", "in_stock", "=", true)

		if err != nil {
			t.Fatalf("Failed to execute in-stock query: %v", err)
		}

		if len(inStockProducts) != 4 {
			t.Fatalf("Expected 4 in-stock products, got %d", len(inStockProducts))
		}
	})

	// Test: Complex select with multiple conditions
	t.Run("ComplexQuery", func(t *testing.T) {
		// Select in-stock furniture under $200
		affordableFurniture, err := db.Select("products", func(row *Row) bool {
			price, priceOk := row.Values["price"].(float64)
			category, catOk := row.Values["category"].(string)
			inStock, stockOk := row.Values["in_stock"].(bool)

			return priceOk && catOk && stockOk &&
				price < 200.0 &&
				category == "Furniture" &&
				inStock
		})

		if err != nil {
			t.Fatalf("Failed to execute complex query: %v", err)
		}

		if len(affordableFurniture) != 1 {
			t.Fatalf("Expected 1 affordable furniture item, got %d", len(affordableFurniture))
		}

		if affordableFurniture[0].Values["name"] != "Desk Chair" {
			t.Fatalf("Expected 'Desk Chair', got '%v'", affordableFurniture[0].Values["name"])
		}
	})

	// Test: SelectByID
	t.Run("SelectByIDQuery", func(t *testing.T) {
		product, err := db.SelectByID("products", 3)

		if err != nil {
			t.Fatalf("Failed to select product by ID: %v", err)
		}

		if product.Values["name"] != "Coffee Mug" {
			t.Fatalf("Expected 'Coffee Mug', got '%v'", product.Values["name"])
		}
	})
}

// TestQueryPerformance tests the performance of queries with a larger dataset
func TestQueryPerformance(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	// Create a temporary database file
	dbPath := "perf_test.db"
	defer os.Remove(dbPath) // Clean up after test

	// Create a new database
	db, err := NewDatabase(dbPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create large table
	columns := []Column{
		{Name: "id", Type: TInteger, NotNull: true},
		{Name: "value", Type: TInteger, NotNull: true},
	}

	err = db.CreateTable("numbers", columns, "id")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert a larger number of rows
	const numRows = 1000
	for i := 1; i <= numRows; i++ {
		err = db.Insert("numbers", map[string]interface{}{
			"id":    int64(i),
			"value": int64(i * 10),
		})
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// Test querying for a specific value
	t.Run("SelectSpecificValue", func(t *testing.T) {
		rows, err := db.SelectWhere("numbers", "value", "=", int64(5000))

		if err != nil {
			t.Fatalf("Failed to select specific value: %v", err)
		}

		if len(rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(rows))
		}

		if rows[0].Values["id"] != int64(500) {
			t.Fatalf("Expected id 500, got %v", rows[0].Values["id"])
		}
	})

	// Test querying for a range of values
	t.Run("SelectValueRange", func(t *testing.T) {
		rows, err := db.Select("numbers", func(row *Row) bool {
			value, ok := row.Values["value"].(int64)
			return ok && value >= 9500 && value <= 9600
		})

		if err != nil {
			t.Fatalf("Failed to select value range: %v", err)
		}

		if len(rows) != 11 {
			t.Fatalf("Expected 11 rows, got %d", len(rows))
		}
	})
}
