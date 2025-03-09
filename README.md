# GDB - A Simple Database Engine in Go

GDB is a lightweight, educational database engine implemented in Go. It provides a simple yet powerful storage engine with key concepts from modern database systems including tables, rows, columns, pages, and typed data. This project was created for educational purposes to understand the inner workings of database systems.

## Features

- **Table-based storage** with schema definition and validation
- **Multiple column types** (Integer, String, Float, Boolean)
- **Page-based storage** for efficient disk I/O
- **In-memory indices** for fast data retrieval 
- **Storage persistence** with automatic recovery
- **ACID-like properties** with basic transaction support
- **SQL-like query capabilities** with condition-based filtering

## Installation

```bash
# Clone the repository
git clone https://github.com/minacio00/gdb.git
cd gdb

# Build the project
go build
```

## Usage Examples

### Basic Usage

```go
package main

import (
	"fmt"
	"log"
	
	"github.com/minacio00/gdb/storageengine"
)

func main() {
	// Create or open a database
	db, err := storageengine.NewDatabase("mydb.db", 4096)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	
	// Create a table
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
	
	// Insert data
	err = db.Insert("users", map[string]interface{}{
		"id": int64(1),
		"name": "John Doe",
		"age": int64(30),
		"is_active": true,
		"salary": float64(75000.50),
	})
	if err != nil {
		log.Fatalf("Failed to insert data: %v", err)
	}
	
	// Query data
	rows, err := db.SelectAll("users")
	if err != nil {
		log.Fatalf("Failed to query data: %v", err)
	}
	
	// Display results
	for _, row := range rows {
		fmt.Printf("User: %s, Age: %v\n", row.Values["name"], row.Values["age"])
	}
}
```

### Advanced Queries

```go
// Select with custom condition
activeUsers, err := db.Select("users", func(row *storageengine.Row) bool {
	active, ok := row.Values["is_active"].(bool)
	return ok && active
})

// Select users over 30
olderUsers, err := db.SelectWhere("users", "age", ">", 30)

// Select users with a specific name
johnUsers, err := db.SelectWhere("users", "name", "=", "John Doe")

// Select users with high salary
highPaidUsers, err := db.SelectWhere("users", "salary", ">=", 70000.0)
```

## Project Structure

The database engine is split into several logical components:

- **types.go**: Core type definitions
- **storage.go**: Disk I/O and page management
- **table.go**: Table operations and schema management
- **row.go**: Row operations and data serialization
- **query.go**: Query operations and filtering

## How It Works

### Page-Based Storage

GDB uses a page-based storage model where data is stored in fixed-size pages (typically 4KB). Each page has a header that describes its content and a body that contains the actual data. Pages can be of different types:

- **Table Pages**: Store table metadata (schema)
- **Data Pages**: Store table rows
- **Index Pages**: Store index data for fast lookups

### Row Storage Format

Rows are stored in a compact binary format:

1. **Null Bitmap**: Indicates which columns are NULL
2. **Column Values**: Each value is serialized according to its type
   - Integers: 8 bytes
   - Floats: 8 bytes
   - Strings: 2-byte length + variable data
   - Booleans: 1 byte

### Memory Management

GDB maintains several in-memory structures for fast access:

1. **Table Registry**: Maps table names to schema information
2. **Row Indices**: B-Trees that map row IDs to physical locations

## Future Enhancements / To-Dos

Here are some enhancements that I would like to add to the project:

### 1. Write-Ahead Logging (WAL)
Implement a WAL system to ensure crash recovery and better ACID compliance.

### 2. SQL Parser
Add a SQL parser to support standard SQL queries instead of the current API.

### 3. Compiled Releases
Provide pre-compiled binaries for major platforms so users don't need to compile the code.

### 4. Secondary Indices
Support for secondary indices to speed up queries on non-primary key columns.

### 5. Query Optimizer
Implement a simple query optimizer that can use indices effectively.

### 6. Transactions
Enhanced transaction support with proper isolation levels.

### 7. Connection Pool
Add a connection pool for concurrent access.

### 8. CLI Tool
Create a command-line interface for interacting with the database.

### 9. Network Protocol
Implement a simple network protocol for client-server operation.


## Educational Value
I've done this project for educational purposes, and it's given me some hands-on experience with
concepts/topics like:

### 1. Data Structures and Algorithms
- B-Trees for indexing
- Serialization formats
- Memory mapping

### 2. Database Concepts
- Query processing
- Transaction management
- Storage engines
- Indexing strategies

### 3. Low-level Systems Programming
- Binary data formats
- Disk I/O

### 4. Go Programming Skills
- Concurrency with mutexes
- Interface implementation
- Error handling
- Binary encoding/decoding

### 5. Software Architecture
- Layered design
- Separation of concerns

---

*Note: GDB is an educational project and not suitable for production use. It lacks many features of a production database such as robust error recovery, security features, and performance optimizations.*