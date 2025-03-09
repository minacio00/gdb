package storageengine

import (
	"encoding/binary"
	"fmt"
	"math"
)

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

	rowID := uint64(db.rowIndices[tableName].Len() + 1)

	row := &Row{
		Values: values,
		RowID:  rowID,
	}

	// Find or create a page for this row
	pageID, rowOffset, err := db.findPageForRow(table, row)
	if err != nil {
		return err
	}

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
func (db *Database) validateRowData(table *Table, values map[string]interface{}) error {
	// Check for required columns
	for _, col := range table.Columns {
		val, exists := values[col.Name]
		if !exists && col.NotNull {
			return fmt.Errorf("missing value for NOT NULL column: %s", col.Name)
		}

		if exists {
			if err := validateValueType(val, col.Type); err != nil {
				return fmt.Errorf("invalid value for column %s: %w", col.Name, err)
			}
		}
	}

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

func validateValueType(value interface{}, colType ColumnType) error {
	if value == nil {
		return nil // NULL value is valid for any column type (unless NOT NULL)
	}

	switch colType {
	case TInteger:
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

	case Tfloat:
		switch value.(type) {
		case float32, float64, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return nil
		}
		return fmt.Errorf("expected numeric value")

	case Tstring:
		switch value.(type) {
		case string:
			return nil
		}
		return fmt.Errorf("expected string value")

	case Tbool:
		switch value.(type) {
		case bool:
			return nil
		}
		return fmt.Errorf("expected boolean value")
	}

	return fmt.Errorf("unknown column type")
}

func (db *Database) findPageForRow(table *Table, row *Row) (uint64, uint16, error) {
	rowData, err := db.serializeRow(row, table)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to serialize row: %w", err)
	}

	rowSize := len(rowData)
	neededSpace := rowSize + 2

	var lastPage *Page
	if table.LastPageID != 0 {
		lastPage, err = db.readPage(table.LastPageID)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to read last data page: %w", err)
		}
	}

	if lastPage == nil || !db.hasEnoughSpace(lastPage, neededSpace) {
		newPage := &Page{
			ID:   db.nextPageID,
			Data: make([]byte, db.pageSize),
		}
		db.nextPageID++

		newPage.Data[0] = byte(PTData)
		binary.LittleEndian.PutUint32(newPage.Data[1:5], table.ID)
		binary.LittleEndian.PutUint16(newPage.Data[5:7], 0)    // No rows yet
		binary.LittleEndian.PutUint64(newPage.Data[7:15], 0)   // No next page yet
		binary.LittleEndian.PutUint16(newPage.Data[15:17], 17) // Free offset starts after header

		if lastPage != nil {
			binary.LittleEndian.PutUint64(lastPage.Data[7:15], newPage.ID)
			if err := db.writePage(lastPage); err != nil {
				return 0, 0, fmt.Errorf("failed to update last page: %w", err)
			}
		} else {
			table.FirstPageID = newPage.ID
		}

		table.LastPageID = newPage.ID
		lastPage = newPage
	}

	return db.addRowToPage(lastPage, rowData, table)
}

func (db *Database) addRowToPage(page *Page, rowData []byte, table *Table) (uint64, uint16, error) {
	rowCount := binary.LittleEndian.Uint16(page.Data[5:7])
	freeOffset := binary.LittleEndian.Uint16(page.Data[15:17])

	binary.LittleEndian.PutUint16(page.Data[freeOffset:freeOffset+2], uint16(len(rowData)))

	copy(page.Data[freeOffset+2:freeOffset+2+uint16(len(rowData))], rowData)

	rowCount++
	binary.LittleEndian.PutUint16(page.Data[5:7], rowCount)

	newFreeOffset := freeOffset + 2 + uint16(len(rowData))
	binary.LittleEndian.PutUint16(page.Data[15:17], newFreeOffset)

	// Write page to disk
	if err := db.writePage(page); err != nil {
		return 0, 0, fmt.Errorf("failed to write page: %w", err)
	}

	return page.ID, freeOffset, nil
}

func (db *Database) serializeRow(row *Row, table *Table) ([]byte, error) {
	nullBitmapSize := (len(table.Columns) + 7) / 8

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

	buffer := make([]byte, nullBitmapSize+dataSize)

	for i, col := range table.Columns {
		val, exists := row.Values[col.Name]
		if !exists || val == nil {
			// Set bit in null bitmap (value is NULL)
			byteIndex := i / 8
			bitIndex := i % 8
			buffer[byteIndex] |= (1 << bitIndex)
		}
	}

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
	}

	return buffer, nil
}

func (db *Database) deserializeRow(data []byte, table *Table) (*Row, error) {
	nullBitmapSize := (len(table.Columns) + 7) / 8

	row := &Row{
		Values: make(map[string]interface{}),
	}

	offset := nullBitmapSize
	for i, col := range table.Columns {
		byteIndex := i / 8
		bitIndex := i % 8
		isNull := (data[byteIndex] & (1 << bitIndex)) != 0

		if isNull {
			continue // Skip NULL values
		}

		switch col.Type {
		case TInteger:
			val := int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
			row.Values[col.Name] = val
			offset += 8
		case Tfloat:
			bits := binary.LittleEndian.Uint64(data[offset : offset+8])
			val := math.Float64frombits(bits)
			row.Values[col.Name] = val
			offset += 8
		case Tstring:
			strLen := binary.LittleEndian.Uint16(data[offset : offset+2])
			offset += 2
			str := string(data[offset : offset+int(strLen)])
			row.Values[col.Name] = str
			offset += int(strLen)
		case Tbool:
			val := data[offset] != 0
			row.Values[col.Name] = val
			offset++
		}
	}

	return row, nil
}

func (db *Database) indexRowsInPage(page *Page, table *Table) error {
	// Get header information
	rowCount := binary.LittleEndian.Uint16(page.Data[5:7])

	offset := uint16(17)

	for i := uint16(0); i < rowCount; i++ {
		// Check if we've reached the end of data
		if offset >= uint16(len(page.Data)) {
			return fmt.Errorf("reached end of page data while reading row %d", i)
		}

		// Read row size
		if offset+2 > uint16(len(page.Data)) {
			return fmt.Errorf("not enough data to read row size")
		}
		rowSize := binary.LittleEndian.Uint16(page.Data[offset : offset+2])

		// Create row index
		rowID := uint64(db.rowIndices[table.Name].Len() + 1)

		rowPtr := RowPtr{
			PageID: page.ID,
			Offset: offset,
		}

		rowIndex := &RowIndex{
			TableID: table.ID,
			RowID:   rowID,
			Ptr:     rowPtr,
		}

		// Add to index
		db.rowIndices[table.Name].ReplaceOrInsert(rowIndex)

		// Move to next row
		offset += 2 + rowSize
	}

	return nil
}
