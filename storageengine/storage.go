package storageengine

import (
	"encoding/binary"
	"fmt"
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
