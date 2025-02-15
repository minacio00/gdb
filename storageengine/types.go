package storageengine

import (
	"os"
	"sync"

	"github.com/google/btree"
)

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
