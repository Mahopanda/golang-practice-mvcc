package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/Mahopanda/golang-mvcc/pkg/mvcc"
)

func main() {
	db := mvcc.NewDatabase()

	// Step 1: Simulate Transactions
	fmt.Println("=== Step 1: Simulate Transactions ===")
	tx1 := db.Begin()
	db.Write(tx1, "key1", "value1")
	fmt.Println("Transaction 1 wrote 'value1'.")

	tx2 := db.Begin()
	value, err := db.ReadWithIsolation(tx2, "key1", mvcc.ReadUncommitted)
	if err != nil {
		fmt.Printf("Transaction 2 failed to read: %v\n", err)
	} else {
		fmt.Printf("Transaction 2 read (Uncommitted): %s\n", value)
	}

	// Commit Transaction 1
	err = db.Commit(tx1)
	if err != nil {
		fmt.Printf("Failed to commit Transaction 1: %v\n", err)
	} else {
		fmt.Println("Transaction 1 committed.")
	}

	// Step 2: Read after commit with Read Committed isolation
	fmt.Println("=== Step 2: Read with Read Committed Isolation ===")
	tx3 := db.Begin()
	value, err = db.ReadWithIsolation(tx3, "key1", mvcc.ReadCommitted)
	if err != nil {
		fmt.Printf("Transaction 3 failed to read: %v\n", err)
	} else {
		fmt.Printf("Transaction 3 read (Committed): %s\n", value)
	}

	// Step 3: Perform Garbage Collection
	fmt.Println("=== Step 3: Garbage Collection ===")
	db.CleanupOldVersions()
	fmt.Println("Garbage collection completed.")

	// Step 4: Verify Garbage Collection
	record := db.GetData()["key1"]
	fmt.Printf("Number of versions after garbage collection: %d\n", len(record.GetVersions()))

	// Step 5: Simulate Concurrent Transactions
	fmt.Println("=== Step 4: Simulate Concurrent Transactions ===")
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		tx := db.Begin()
		db.Write(tx, "key1", "value2")
		db.Commit(tx)
		log.Println("Transaction 4 wrote 'value2' and committed.")
	}()

	go func() {
		defer wg.Done()
		tx := db.Begin()
		db.Write(tx, "key1", "value3")
		db.Commit(tx)
		log.Println("Transaction 5 wrote 'value3' and committed.")
	}()

	wg.Wait()

	// Verify Concurrent Transactions
	tx6 := db.Begin()
	value, err = db.ReadWithIsolation(tx6, "key1", mvcc.RepeatableRead)
	if err != nil {
		fmt.Printf("Transaction 6 failed to read: %v\n", err)
	} else {
		fmt.Printf("Transaction 6 read (RepeatableRead): %s\n", value)
	}

	fmt.Println("=== Program Completed ===")
}
