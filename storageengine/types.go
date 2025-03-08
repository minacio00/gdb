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
	Type PageType
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

// Item represents a database record that can be stored in the B-tree
type Item struct {
	Key   int64
	Value []byte
	// Page ID where this item is stored
	pageID uint64
}

// Less implements btree.Item interface
func (i *Item) Less(than btree.Item) bool {
	return i.Key < than.(*Item).Key
}

// Page represents our on-disk storage unit
type Page struct {
	ID       uint64
	Data     []byte
	NumItems int
}

// Database represents our clustered database
type Database struct {
	file       *os.File
	tree       *btree.BTree
	pageSize   int
	nextPageID uint64
	mu         sync.RWMutex
}
