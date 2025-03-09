package storageengine

import (
	"os"
	"sync"

	"github.com/google/btree"
)

type ColumnType byte

const (
	TInteger = iota
	Tstring
	Tfloat
	Tbool
)

type Column struct {
	Name    string
	Type    ColumnType
	NotNull bool
}

type Table struct {
	ID          uint32
	Name        string
	Columns     []Column
	PK          string
	FirstPageID uint64
	LastPageID  uint64
}
type PageType byte

const (
	PTFree = iota
	PTTable
	PTData
	PTIndex
)

type Page struct {
	ID   uint64
	Data []byte
}

type PageHeader struct {
	Type       PageType
	TableID    uint32
	RowCount   uint16
	nextPageID uint64
	FreeOffset uint16
}

type Row struct {
	Values map[string]interface{}
	RowID  uint64
}
type RowPtr struct {
	PageID uint64
	Offset uint16
}

type RowIndex struct {
	TableID uint32
	RowID   uint64
	Ptr     RowPtr
}

func (ri *RowIndex) Less(than btree.Item) bool {
	other := than.(*RowIndex)
	if ri.TableID != other.TableID {
		return ri.TableID < other.TableID
	}
	return ri.RowID < other.RowID
}

type Database struct {
	file        *os.File
	pageSize    int
	nextPageID  uint64
	mu          sync.RWMutex
	tables      map[string]*Table
	tableIDMap  map[string]*Table
	rowIndices  map[string]*btree.BTree
	nextTableID uint32
}
