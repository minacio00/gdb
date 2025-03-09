package storageengine

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"

	"github.com/google/btree"
)

// NewDatabase creates a new clustered database
func NewDatabase(path string, pageSize int) (*Database, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open database file: %w", err)
	}
	db := &Database{
		file:        file,
		nextPageID:  0,
		tables:      make(map[string]*Table),
		tableIDMap:  make(map[string]*Table),
		rowIndices:  make(map[string]*btree.BTree),
		nextTableID: 1,
	}
	// Initialize or load existing database
	if info, err := file.Stat(); err != nil {
		return nil, err
	} else if info.Size() > 0 {
		if err := db.loadExistingData(); err != nil {
			return nil, err
		}
	}

	return db, nil
}
func (db *Database) CreateTable(tableName string, columns []Column, primaryKey string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Check if table already exists
	if _, exists := db.tables[tableName]; exists {
		return fmt.Errorf("table already exists: %s", tableName)
	}

	// Validate primary key
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

	// Create a B-tree index for this table
	db.rowIndices[table.Name] = btree.New(32)

	// Create table metadata page
	tablePage := &Page{
		ID:   db.nextPageID,
		Data: make([]byte, db.pageSize),
	}
	db.nextPageID++

	// Initialize page header
	tablePage.Data[0] = byte(PTTable)
	binary.LittleEndian.PutUint32(tablePage.Data[1:5], table.ID)
	binary.LittleEndian.PutUint16(tablePage.Data[5:7], 0)    // RowCount is always 0 for table pages
	binary.LittleEndian.PutUint64(tablePage.Data[7:15], 0)   // No next page initially
	binary.LittleEndian.PutUint16(tablePage.Data[15:17], 17) // Free offset starts after header

	// Serialize table schema
	if err := serializeTable(table, tablePage); err != nil {
		return fmt.Errorf("failed to serialize table: %w", err)
	}

	// Create first data page for this table
	dataPage := &Page{
		ID:   db.nextPageID,
		Data: make([]byte, db.pageSize),
	}
	db.nextPageID++

	// Initialize data page header
	dataPage.Data[0] = byte(PTData)
	binary.LittleEndian.PutUint32(dataPage.Data[1:5], table.ID)
	binary.LittleEndian.PutUint16(dataPage.Data[5:7], 0)    // No rows yet
	binary.LittleEndian.PutUint64(dataPage.Data[7:15], 0)   // No next page yet
	binary.LittleEndian.PutUint16(dataPage.Data[15:17], 17) // Free offset starts after header

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

func deserializeTable(page *Page) (*Table, error) {
	// Start after page header
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

func serializeTable(table *Table, page *Page) error {
	offset := uint16(17)
	nameLen := uint16(len(table.Name))
	// write table name lenght to the page
	binary.LittleEndian.PutUint16(page.Data[offset:offset+2], nameLen)
	offset += 2
	copy(page.Data[offset:offset+nameLen], []byte(table.Name))
	offset += nameLen

	// write number of columns
	colCount := uint16(len(table.Columns))
	binary.LittleEndian.PutUint16(page.Data[offset:offset+2], colCount)
	offset += 2

	pkLen := uint16(len(table.PK))
	binary.LittleEndian.PutUint16(page.Data[offset:offset+2], pkLen)
	offset += 2

	// Write primary key name
	copy(page.Data[offset:offset+pkLen], table.PK)
	offset += pkLen

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
	binary.LittleEndian.PutUint16(page.Data[15:17], offset)

	return nil

}

// writePage writes a page to disk
func (db *Database) writePage(page *Page) error {
	offset := int64(page.ID) * int64(db.pageSize)
	_, err := db.file.WriteAt(page.Data, offset)
	return err
}

// readPage reads a page from disk
func (db *Database) readPage(pageID uint64) (*Page, error) {
	page := &Page{
		ID:   pageID,
		Data: make([]byte, db.pageSize),
	}

	offset := int64(pageID) * int64(db.pageSize)
	_, err := db.file.ReadAt(page.Data, offset)
	if err != nil {
		return nil, err
	}

	return page, nil
}

// Insert adds a row to a table
func (db *Database) Insert(tableName string, values map[string]interface{}) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Find table
	table, exists := db.tables[tableName]
	if !exists {
		return fmt.Errorf("table not found: %s", tableName)
	}

	// Validate values against schema
	if err := db.validateRowData(table, values); err != nil {
		return err
	}

	// Generate row ID (in a real system, this might be based on the primary key)
	rowID := uint64(db.rowIndices[tableName].Len() + 1)

	// Create row
	row := &Row{
		Values: values,
		RowID:  rowID,
	}

	// Find or create a page for this row
	pageID, rowOffset, err := db.findPageForRow(table, row)
	if err != nil {
		return err
	}

	// Update the in-memory index
	rowPtr := RowPtr{
		PageID: pageID,
		Offset: rowOffset,
	}

	rowIndex := &RowIndex{
		TableID: table.ID,
		RowID:   rowID,
		Ptr:     rowPtr,
	}
	db.rowIndices[tableName].ReplaceOrInsert(rowIndex)

	return nil
}

// validateRowData validates that the provided values conform to the table schema
func (db *Database) validateRowData(table *Table, values map[string]interface{}) error {
	// Check for required columns
	for _, col := range table.Columns {
		val, exists := values[col.Name]
		if !exists && col.NotNull {
			return fmt.Errorf("missing value for NOT NULL column: %s", col.Name)
		}

		if exists {
			// Validate value type
			if err := validateValueType(val, col.Type); err != nil {
				return fmt.Errorf("invalid value for column %s: %w", col.Name, err)
			}
		}
	}

	// Check for unknown columns
	for colName := range values {
		found := false
		for _, col := range table.Columns {
			if col.Name == colName {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("unknown column: %s", colName)
		}
	}

	return nil
}

// validateValueType checks if a value matches its expected column type
func validateValueType(value interface{}, colType ColumnType) error {
	if value == nil {
		return nil // NULL value is valid for any column type (unless NOT NULL)
	}

	switch colType {
	case TypeInteger:
		switch v := value.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return nil
		case float32, float64:
			// Check if float is actually an integer
			f := v.(float64)
			if f == math.Trunc(f) {
				return nil
			}
		}
		return fmt.Errorf("expected integer value")

	case TypeFloat:
		switch value.(type) {
		case float32, float64, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return nil
		}
		return fmt.Errorf("expected numeric value")

	case TypeString:
		switch value.(type) {
		case string:
			return nil
		}
		return fmt.Errorf("expected string value")

	case TypeBool:
		switch value.(type) {
		case bool:
			return nil
		}
		return fmt.Errorf("expected boolean value")

	case TypeBlob:
		switch value.(type) {
		case []byte:
			return nil
		}
		return fmt.Errorf("expected blob value")
	}

	return fmt.Errorf("unknown column type")
}

// findPageForRow finds or creates a page with enough space for the row
func (db *Database) findPageForRow(table *Table, row *Row) (uint64, uint16, error) {
	// Serialize row to determine its size
	rowData, err := db.serializeRow(row, table)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to serialize row: %w", err)
	}

	// We need space for: row data + 2 bytes for row size
	rowSize := len(rowData)
	neededSpace := rowSize + 2

	// Start with the last data page
	var lastPage *Page
	if table.LastPageID != 0 {
		lastPage, err = db.readPage(table.LastPageID)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to read last data page: %w", err)
		}
	}

	// If there's no last page or not enough space, create a new page
	if lastPage == nil || !db.hasEnoughSpace(lastPage, neededSpace) {
		// Create new page
		newPage := &Page{
			ID:   db.nextPageID,
			Data: make([]byte, db.pageSize),
		}
		db.nextPageID++

		// Initialize new page header
		newPage.Data[0] = byte(PageTypeData)
		binary.LittleEndian.PutUint32(newPage.Data[1:5], table.ID)
		binary.LittleEndian.PutUint16(newPage.Data[5:7], 0)    // No rows yet
		binary.LittleEndian.PutUint64(newPage.Data[7:15], 0)   // No next page yet
		binary.LittleEndian.PutUint16(newPage.Data[15:17], 17) // Free offset starts after header

		// If we had a last page, update its next page pointer
		if lastPage != nil {
			binary.LittleEndian.PutUint64(lastPage.Data[7:15], newPage.ID)
			if err := db.writePage(lastPage); err != nil {
				return 0, 0, fmt.Errorf("failed to update last page: %w", err)
			}
		} else {
			// This is the first data page for the table
			table.FirstPageID = newPage.ID
		}

		// Update table's last page
		table.LastPageID = newPage.ID
		lastPage = newPage
	}

	// Now we have a page with enough space, add the row
	return db.addRowToPage(lastPage, rowData, table)
}

// hasEnoughSpace checks if a page has enough space for a value of given size
func (db *Database) hasEnoughSpace(page *Page, neededSpace int) bool {
	// Read free offset
	freeOffset := binary.LittleEndian.Uint16(page.Data[15:17])

	// Check if there's enough space (leave some margin for safety)
	return int(freeOffset)+neededSpace+4 <= db.pageSize
}

// addRowToPage adds a row to a page and returns the row's location
func (db *Database) addRowToPage(page *Page, rowData []byte, table *Table) (uint64, uint16, error) {
	// Get current header values
	rowCount := binary.LittleEndian.Uint16(page.Data[5:7])
	freeOffset := binary.LittleEndian.Uint16(page.Data[15:17])

	// Write row size
	binary.LittleEndian.PutUint16(page.Data[freeOffset:freeOffset+2], uint16(len(rowData)))

	// Write row data
	copy(page.Data[freeOffset+2:freeOffset+2+uint16(len(rowData))], rowData)

	// Update row count
	rowCount++
	binary.LittleEndian.PutUint16(page.Data[5:7], rowCount)

	// Update free offset
	newFreeOffset := freeOffset + 2 + uint16(len(rowData))
	binary.LittleEndian.PutUint16(page.Data[15:17], newFreeOffset)

	// Write page to disk
	if err := db.writePage(page); err != nil {
		return 0, 0, fmt.Errorf("failed to write page: %w", err)
	}

	return page.ID, freeOffset, nil
}

// serializeRow serializes a row according to the table schema
func (db *Database) serializeRow(row *Row, table *Table) ([]byte, error) {
	// Calculate null bitmap size (1 bit per column, rounded up to bytes)
	nullBitmapSize := (len(table.Columns) + 7) / 8

	// First pass: calculate required size
	dataSize := 0
	for _, col := range table.Columns {
		val, exists := row.Values[col.Name]
		if !exists || val == nil {
			continue // NULL value, needs no space except in bitmap
		}

		switch col.Type {
		case TInteger:
			dataSize += 8 // int64
		case Tfloat:
			dataSize += 8 // float64
		case Tstring:
			str, ok := val.(string)
			if !ok {
				return nil, fmt.Errorf("invalid type for string column %s", col.Name)
			}
			dataSize += 2 + len(str) // 2 bytes for length + string data
		case Tbool:
			dataSize += 1 // 1 byte	
		}
	}

	// Allocate buffer
	buffer := make([]byte, nullBitmapSize+dataSize)

	// Write null bitmap
	for i, col := range table.Columns {
		val, exists := row.Values[col.Name]
		if !exists || val == nil {
			// Set bit in null bitmap (value is NULL)
			byteIndex := i / 8
			bitIndex := i % 8
			buffer[byteIndex] |= (1 << bitIndex)
		}
	}

	// Write values
	offset := nullBitmapSize
	for _, col := range table.Columns {
		val, exists := row.Values[col.Name]
		if !exists || val == nil {
			continue // Skip NULL values
		}

		switch col.Type {
		case TInteger:
			var v int64
			switch val := val.(type) {
			case int:
				v = int64(val)
			case int8:
				v = int64(val)
			case int16:
				v = int64(val)
			case int32:
				v = int64(val)
			case int64:
				v = val
			case uint:
				v = int64(val)
			case uint8:
				v = int64(val)
			case uint16:
				v = int64(val)
			case uint32:
				v = int64(val)
			case uint64:
				v = int64(val)
			case float64:
				v = int64(val)
			default:
				return nil, fmt.Errorf("invalid type for integer column %s", col.Name)
			}
			binary.LittleEndian.PutUint64(buffer[offset:offset+8], uint64(v))
			offset += 8

		case Tfloat:
			var v float64
			switch val := val.(type) {
			case float32:
				v = float64(val)
			case float64:
				v = val
			case int:
				v = float64(val)
			case int8:
				v = float64(val)
			case int16:
				v = float64(val)
			case int32:
				v = float64(val)
			case int64:
				v = float64(val)
			case uint:
				v = float64(val)
			case uint8:
				v = float64(val)
			case uint16:
				v = float64(val)
			case uint32:
				v = float64(val)
			case uint64:
				v = float64(val)
			default:
				return nil, fmt.Errorf("invalid type for float column %s", col.Name)
			}
			bits := math.Float64bits(v)
			binary.LittleEndian.PutUint64(buffer[offset:offset+8], bits)
			offset += 8

		case Tstring:
			str, ok := val.(string)
			if !ok {
				return nil, fmt.Errorf("invalid type for string column %s", col.Name)
			}
			binary.LittleEndian.PutUint16(buffer[offset:offset+2], uint16(len(str)))
			offset += 2
			copy(buffer[offset:offset+len(str)], str)
			offset += len(str)

		case Tbool:
			b, ok := val.(bool)
			if !ok {
				return nil, fmt.Errorf("invalid type for boolean column %s", col.Name)
			}
			if b {
				buffer[offset] = 1
			} else {
				buffer[offset] = 0
			}
			offset++	
	}

	return buffer, nil
}

// loadExistingData loads existing database content into memory
func (db *Database) loadExistingData() error {
	fileInfo, err := db.file.Stat()
	if err != nil {
		return err
	}

	numPages := fileInfo.Size() / int64(db.pageSize)
	for pageID := uint64(0); pageID < uint64(numPages); pageID++ {
		_, err := db.readPage(pageID)
		if err != nil {
			return err
		}

		// item, err := db.deserializeItem(page)
		// if err != nil {
		// 	return err
		// }

		// db.tree.ReplaceOrInsert(item)
		// //if this condition does not get satifies it means there is no more pages in the diskj
		// if pageID >= db.nextPageID {
		// 	db.nextPageID = pageID + 1
		// }
	}

	return nil
}

// Close closes the database
func (db *Database) Close() error {
	return db.file.Close()
}
