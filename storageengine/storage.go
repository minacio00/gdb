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
func (db *Database) CreateTable(table *Table) error {
	Tid := db.nextTableID
	table.ID = Tid

	return nil
}
func newPage(id uint64, ptype PageType, TId *uint32, data []byte) *Page {
	header := &PageHeader{
		Type:     ptype,
		TableID:  *TId,
		RowCount: 0,
	}
	page := &Page{
		ID: id,
	}
	//serialize header
	page.Data[0] = byte(header.Type)
	binary.LittleEndian.PutUint32(page.Data[1:5], header.TableID)
	binary.LittleEndian.PutUint16(page.Data[5:7], header.RowCount)
	binary.LittleEndian.PutUint64(page.Data[7:15], header.nextPageID)
	binary.LittleEndian.PutUint16(page.Data[15:17], header.FreeOffset)
	switch ptype {
	case PTTable:
		serializeTable()
	}

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
func NewPageHeader(ptype PageType, TId *uint32) *PageHeader {
	return &PageHeader{
		Type:     ptype,
		TableID:  *TId,
		RowCount: 0,
	}

}

// // Insert adds a new record to the database
// func (db *Database) Insert(key int64, value []byte) error {
// 	db.mu.Lock()
// 	defer db.mu.Unlock()

// 	// Create new item
// 	item := &Item{
// 		Key:   key,
// 		Value: value,
// 	}

// 	// Allocate new page for the item
// 	page := &Page{
// 		ID:   db.nextPageID,
// 		Data: make([]byte, db.pageSize),
// 	}
// 	db.nextPageID++

// 	// Serialize item into page
// 	if err := db.serializeItem(item, page); err != nil {
// 		return fmt.Errorf("failed to serialize item: %w", err)
// 	}

// 	// Write page to disk
// 	if err := db.writePage(page); err != nil {
// 		return fmt.Errorf("failed to write page: %w", err)
// 	}

// 	// Update item with page reference
// 	item.pageID = page.ID

// 	// Insert into in-memory B-tree
// 	db.tree.ReplaceOrInsert(item)

// 	return nil
// }

// Get retrieves a record by key
// func (db *Database) Get(key int64) ([]byte, error) {
// 	db.mu.RLock()
// 	defer db.mu.RUnlock()

// 	// Search in B-tree
// 	item := db.tree.Get(&Item{Key: key})
// 	if item == nil {
// 		return nil, fmt.Errorf("key not found: %d", key)
// 	}

// 	// Load page from disk
// 	page, err := db.readPage(item.(*Item).pageID)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to read page: %w", err)
// 	}

// 	// Deserialize item from page
// 	deserializedItem, err := db.deserializeItem(page)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to deserialize item: %w", err)
// 	}

// 	return deserializedItem.Value, nil
// }

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

// serializeItem serializes an item into a page
func (db *Database) serializeItem(item *Item, page *Page) error {
	// Format: [key(8 bytes)][value_length(4 bytes)][value(N bytes)]
	if 8+4+len(item.Value) > db.pageSize {
		return fmt.Errorf("item too large for page")
	}

	binary.LittleEndian.PutUint64(page.Data[0:8], uint64(item.Key))
	binary.LittleEndian.PutUint32(page.Data[8:12], uint32(len(item.Value)))
	copy(page.Data[12:], item.Value)

	return nil
}

// deserializeItem deserializes an item from a page
func (db *Database) deserializeItem(page *Page) (*Item, error) {
	key := int64(binary.LittleEndian.Uint64(page.Data[0:8]))
	valueLen := binary.LittleEndian.Uint32(page.Data[8:12])

	value := make([]byte, valueLen)
	copy(value, page.Data[12:12+valueLen])

	return &Item{
		Key:    key,
		Value:  value,
		pageID: page.ID,
	}, nil
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
