package storageengine

import (
	"encoding/binary"
	"fmt"

	"github.com/google/btree"
)

// CreateTable creates a new table in the database
func (db *Database) CreateTable(tableName string, columns []Column, primaryKey string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.tables[tableName]; exists {
		return fmt.Errorf("table already exists: %s", tableName)
	}

	if primaryKey != "" {
		hasPK := false
		for _, col := range columns {
			if col.Name == primaryKey {
				hasPK = true
				break
			}
		}
		if !hasPK {
			return fmt.Errorf("primary key column not found: %s", primaryKey)
		}
	}

	// Create table object
	table := &Table{
		ID:      db.nextTableID,
		Name:    tableName,
		Columns: columns,
		PK:      primaryKey,
	}
	db.nextTableID++

	db.rowIndices[table.Name] = btree.New(32)

	// Create and initialize table metadata page
	tablePage := &Page{
		ID:   db.nextPageID,
		Data: make([]byte, db.pageSize),
	}
	db.nextPageID++

	tablePage.Data[0] = byte(PTTable)
	binary.LittleEndian.PutUint32(tablePage.Data[1:5], table.ID)
	binary.LittleEndian.PutUint16(tablePage.Data[5:7], 0)    // RowCount is always 0 for table pages
	binary.LittleEndian.PutUint64(tablePage.Data[7:15], 0)   // No next page initially
	binary.LittleEndian.PutUint16(tablePage.Data[15:17], 17) // Free offset starts after header

	if err := serializeTable(table, tablePage); err != nil {
		return fmt.Errorf("failed to serialize table: %w", err)
	}

	// Create and initialize first data page for this table
	dataPage := &Page{
		ID:   db.nextPageID,
		Data: make([]byte, db.pageSize),
	}
	db.nextPageID++

	dataPage.Data[0] = byte(PTData)
	binary.LittleEndian.PutUint32(dataPage.Data[1:5], table.ID)
	binary.LittleEndian.PutUint16(dataPage.Data[5:7], 0)
	binary.LittleEndian.PutUint64(dataPage.Data[7:15], 0)
	binary.LittleEndian.PutUint16(dataPage.Data[15:17], 17)

	// Update table with page IDs
	table.FirstPageID = dataPage.ID
	table.LastPageID = dataPage.ID

	// Write pages to disk
	if err := db.writePage(tablePage); err != nil {
		return fmt.Errorf("failed to write table page: %w", err)
	}

	if err := db.writePage(dataPage); err != nil {
		return fmt.Errorf("failed to write data page: %w", err)
	}

	// Add table to in-memory maps
	db.tables[table.Name] = table
	db.tableIDMap[table.Name] = table

	return nil
}

// GetTableSchema returns the schema for a table
func (db *Database) GetTableSchema(tableName string) (*Table, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	table, exists := db.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	return table, nil
}

// serializeTable serializes a table schema into a page
func serializeTable(table *Table, page *Page) error {
	offset := uint16(17)
	nameLen := uint16(len(table.Name))

	// Write table name length
	binary.LittleEndian.PutUint16(page.Data[offset:offset+2], nameLen)
	offset += 2

	// Write table name
	copy(page.Data[offset:offset+nameLen], []byte(table.Name))
	offset += nameLen

	// Write number of columns
	colCount := uint16(len(table.Columns))
	binary.LittleEndian.PutUint16(page.Data[offset:offset+2], colCount)
	offset += 2

	// Write primary key length
	pkLen := uint16(len(table.PK))
	binary.LittleEndian.PutUint16(page.Data[offset:offset+2], pkLen)
	offset += 2

	// Write primary key name
	copy(page.Data[offset:offset+pkLen], table.PK)
	offset += pkLen

	// Write column definitions
	for _, col := range table.Columns {
		// Write column name length
		colNameLen := uint16(len(col.Name))
		binary.LittleEndian.PutUint16(page.Data[offset:offset+2], colNameLen)
		offset += 2

		// Write column name
		copy(page.Data[offset:offset+colNameLen], col.Name)
		offset += colNameLen

		// Write column type
		page.Data[offset] = byte(col.Type)
		offset++

		// Write column flags (NotNull for now)
		if col.NotNull {
			page.Data[offset] = 1
		} else {
			page.Data[offset] = 0
		}
		offset++
	}

	// Update free offset in page header
	binary.LittleEndian.PutUint16(page.Data[15:17], offset)

	return nil
}

// deserializeTable deserializes a table schema from a page
func deserializeTable(page *Page) (*Table, error) {
	offset := uint16(17)

	// Read table ID from page header
	tableID := binary.LittleEndian.Uint32(page.Data[1:5])

	// Read table name length
	nameLen := binary.LittleEndian.Uint16(page.Data[offset : offset+2])
	offset += 2

	// Read table name
	tableName := string(page.Data[offset : offset+nameLen])
	offset += nameLen

	// Read number of columns
	colCount := binary.LittleEndian.Uint16(page.Data[offset : offset+2])
	offset += 2

	// Read primary key name length
	pkLen := binary.LittleEndian.Uint16(page.Data[offset : offset+2])
	offset += 2

	// Read primary key name
	primaryKey := string(page.Data[offset : offset+pkLen])
	offset += pkLen

	// Read columns
	columns := make([]Column, colCount)
	for i := range columns {
		// Read column name length
		colNameLen := binary.LittleEndian.Uint16(page.Data[offset : offset+2])
		offset += 2

		// Read column name
		colName := string(page.Data[offset : offset+colNameLen])
		offset += colNameLen

		// Read column type
		colType := ColumnType(page.Data[offset])
		offset++

		// Read column flags
		notNull := page.Data[offset] != 0
		offset++

		columns[i] = Column{
			Name:    colName,
			Type:    colType,
			NotNull: notNull,
		}
	}

	// Create and return table
	table := &Table{
		ID:      tableID,
		Name:    tableName,
		Columns: columns,
		PK:      primaryKey,
	}

	return table, nil
}
