package main

import (
	"fmt"

	"github.com/minacio00/gdb/storageengine"
)

func initDB(path string, pageSize int) (*storageengine.Database, error) {
	db, err := storageengine.NewDatabase(path, pageSize)
	if err != nil {
		return nil, err
	}
	return db, nil
}
func main() {
	databasefile := "./database"
	page := 4096
	db, err := initDB(databasefile, page)
	if err != nil {
		panic(err.Error())
	}

	data := struct {
		key   int64
		value []byte
	}{key: 1, value: []byte("amado")}

	db.Insert(data.key, data.value)
	arr, err := db.Get(1)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(string(arr))

}
