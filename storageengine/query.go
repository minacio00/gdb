package storageengine

import (
	"encoding/binary"
	"fmt"

	"github.com/google/btree"
)

func (db *Database) Select(tableName string, condition func(row *Row) bool) ([]*Row, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	table, exists := db.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	var result []*Row

	index := db.rowIndices[tableName]
	if index == nil {
		return nil, fmt.Errorf("index not found for table: %s", tableName)
	}

	index.Ascend(func(item btree.Item) bool {
		rowIndex := item.(*RowIndex)

		page, err := db.readPage(rowIndex.Ptr.PageID)
		if err != nil {
			return true
		}

		rowSize := binary.LittleEndian.Uint16(page.Data[rowIndex.Ptr.Offset : rowIndex.Ptr.Offset+2])

		rowData := page.Data[rowIndex.Ptr.Offset+2 : rowIndex.Ptr.Offset+2+rowSize]

		row, err := db.deserializeRow(rowData, table)
		if err != nil {
			return true
		}

		row.RowID = rowIndex.RowID

		if condition == nil || condition(row) {
			result = append(result, row)
		}

		return true
	})

	return result, nil
}

func (db *Database) SelectAll(tableName string) ([]*Row, error) {
	return db.Select(tableName, nil)
}

func (db *Database) SelectByID(tableName string, id uint64) (*Row, error) {
	rows, err := db.Select(tableName, func(row *Row) bool {
		return row.RowID == id
	})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("row not found with ID: %d", id)
	}
	return rows[0], nil
}

func (db *Database) SelectWhere(tableName string, columnName string, op string, value interface{}) ([]*Row, error) {
	// Get table schema to validate column
	table, err := db.GetTableSchema(tableName)
	if err != nil {
		return nil, err
	}

	var targetCol *Column
	for _, col := range table.Columns {
		if col.Name == columnName {
			targetCol = &col
			break
		}
	}

	if targetCol == nil {
		return nil, fmt.Errorf("column not found: %s", columnName)
	}

	if err := validateValueType(value, targetCol.Type); err != nil {
		return nil, fmt.Errorf("invalid value for column %s: %w", columnName, err)
	}

	var condition func(row *Row) bool

	switch op {
	case "=", "==":
		condition = func(row *Row) bool {
			rowVal, exists := row.Values[columnName]
			if !exists {
				return false
			}
			return compareValues(rowVal, value) == 0
		}
	case ">":
		condition = func(row *Row) bool {
			rowVal, exists := row.Values[columnName]
			if !exists {
				return false
			}
			return compareValues(rowVal, value) > 0
		}
	case ">=":
		condition = func(row *Row) bool {
			rowVal, exists := row.Values[columnName]
			if !exists {
				return false
			}
			return compareValues(rowVal, value) >= 0
		}
	case "<":
		condition = func(row *Row) bool {
			rowVal, exists := row.Values[columnName]
			if !exists {
				return false
			}
			return compareValues(rowVal, value) < 0
		}
	case "<=":
		condition = func(row *Row) bool {
			rowVal, exists := row.Values[columnName]
			if !exists {
				return false
			}
			return compareValues(rowVal, value) <= 0
		}
	case "!=", "<>":
		condition = func(row *Row) bool {
			rowVal, exists := row.Values[columnName]
			if !exists {
				return false
			}
			return compareValues(rowVal, value) != 0
		}
	case "LIKE":
		strValue, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("LIKE operator requires string value")
		}

		condition = func(row *Row) bool {
			rowVal, exists := row.Values[columnName]
			if !exists {
				return false
			}

			rowStr, ok := rowVal.(string)
			if !ok {
				return false
			}

			return matchLike(rowStr, strValue)
		}
	default:
		return nil, fmt.Errorf("unsupported operator: %s", op)
	}

	return db.Select(tableName, condition)
}

// compareValues compares two values of potentially different types
// Returns: -1 if a < b, 0 if a == b, 1 if a > b
func compareValues(a, b interface{}) int {
	// Handle nil values
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	var aNum, bNum float64
	var aIsNum, bIsNum bool

	switch v := a.(type) {
	case int:
		aNum, aIsNum = float64(v), true
	case int64:
		aNum, aIsNum = float64(v), true
	case float64:
		aNum, aIsNum = v, true
	}

	switch v := b.(type) {
	case int:
		bNum, bIsNum = float64(v), true
	case int64:
		bNum, bIsNum = float64(v), true
	case float64:
		bNum, bIsNum = v, true
	}

	if aIsNum && bIsNum {
		if aNum < bNum {
			return -1
		} else if aNum > bNum {
			return 1
		}
		return 0
	}

	aStr, aIsStr := a.(string)
	bStr, bIsStr := b.(string)

	if aIsStr && bIsStr {
		if aStr < bStr {
			return -1
		} else if aStr > bStr {
			return 1
		}
		return 0
	}

	aBool, aIsBool := a.(bool)
	bBool, bIsBool := b.(bool)

	if aIsBool && bIsBool {
		if aBool == bBool {
			return 0
		} else if aBool {
			return 1
		} else {
			return -1
		}
	}

	return 0
}

// matchLike performs a simple LIKE comparison with % wildcards
func matchLike(str, pattern string) bool {
	// TODO: Implement a more robust LIKE matching

	// Case: pattern is just %
	if pattern == "%" {
		return true
	}

	// Case: pattern starts with %
	if len(pattern) > 0 && pattern[0] == '%' {
		if len(pattern) == 1 {
			return true
		}

		suffix := pattern[1:]
		// Check if string ends with suffix
		if len(suffix) > 0 && suffix[len(suffix)-1] == '%' {
			// Pattern is %...%
			middle := suffix[:len(suffix)-1]
			return len(middle) > 0 && contains(str, middle)
		}

		return len(str) >= len(suffix) && str[len(str)-len(suffix):] == suffix
	}

	// Case: pattern ends with %
	if len(pattern) > 0 && pattern[len(pattern)-1] == '%' {
		prefix := pattern[:len(pattern)-1]
		return len(str) >= len(prefix) && str[:len(prefix)] == prefix
	}

	return str == pattern
}

func contains(str, substr string) bool {
	for i := 0; i <= len(str)-len(substr); i++ {
		if str[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
