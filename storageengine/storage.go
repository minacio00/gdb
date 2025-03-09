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
		pageSize:    pageSize,
		nextPageID:  0,
		tables:      make(map[string]*Table),
		tableIDMap:  make(map[string]*Table),
		rowIndices:  make(map[string]*btree.BTree),
		nextTableID: 1,
	}
	if info, err := file.Stat(); err != nil {
		return nil, err
	} else if info.Size() > 0 {
		if err := db.loadExistingData(); err != nil {
			return nil, err
		}
	}

	return db, nil
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

	// First pass: Load table definitions
	for pageID := uint64(0); pageID < uint64(numPages); pageID++ {
		page, err := db.readPage(pageID)
		if err != nil {
			return fmt.Errorf("failed to read page %d: %w", pageID, err)
		}

		if pageID >= db.nextPageID {
			db.nextPageID = pageID + 1
		}

		if len(page.Data) == 0 {
			continue // Skip empty pages
		}

		pageType := PageType(page.Data[0])

		if pageType == PTTable {
			table, err := deserializeTable(page)
			if err != nil {
				return fmt.Errorf("failed to deserialize table on page %d: %w", pageID, err)
			}

			db.rowIndices[table.Name] = btree.New(32)

			// Add table to maps
			db.tables[table.Name] = table
			db.tableIDMap[table.Name] = table

			if table.ID >= db.nextTableID {
				db.nextTableID = table.ID + 1
			}
		}
	}

	// Second pass: Process data pages and build indices
	for pageID := uint64(0); pageID < uint64(numPages); pageID++ {
		page, err := db.readPage(pageID)
		if err != nil {
			return fmt.Errorf("failed to read page %d: %w", pageID, err)
		}

		if len(page.Data) == 0 {
			continue
		}

		pageType := PageType(page.Data[0])

		if pageType == PTData {
			// Extract table ID from page header
			tableID := binary.LittleEndian.Uint32(page.Data[1:5])
			nextPageID := binary.LittleEndian.Uint64(page.Data[7:15])

			// Find corresponding table by ID
			var targetTable *Table
			for _, table := range db.tables {
				if table.ID == tableID {
					targetTable = table
					break
				}
			}

			if targetTable == nil {
				return fmt.Errorf("data page %d references unknown table ID %d", pageID, tableID)
			}

			if targetTable.FirstPageID == 0 {
				targetTable.FirstPageID = pageID
			}

			if nextPageID == 0 {
				targetTable.LastPageID = pageID
			}

			err = db.indexRowsInPage(page, targetTable)
			if err != nil {
				return fmt.Errorf("failed to index rows in page %d: %w", pageID, err)
			}
		}
	}

	return nil
}

// Close closes the database
func (db *Database) Close() error {
	return db.file.Close()
}

// ListTables returns names of all tables in the database
func (db *Database) ListTables() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	result := make([]string, 0, len(db.tables))
	for name := range db.tables {
		result = append(result, name)
	}
	return result
}

// GetRowCount returns the number of rows in a table
func (db *Database) GetRowCount(tableName string) (int, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	index, exists := db.rowIndices[tableName]
	if !exists {
		return 0, fmt.Errorf("table not found: %s", tableName)
	}

	return index.Len(), nil
}

// hasEnoughSpace checks if a page has enough space for a value of given size
func (db *Database) hasEnoughSpace(page *Page, neededSpace int) bool {
	// Read free offset
	freeOffset := binary.LittleEndian.Uint16(page.Data[15:17])

	// Check if there's enough space (leave some margin for safety)
	return int(freeOffset)+neededSpace+4 <= db.pageSize
}
