// Individual file modifications for the MVCC project

package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/Mahopanda/golang-mvcc/pkg/mvcc"
)

func main() {
	db := mvcc.NewDatabase()

	// Simulate basic transactions
	fmt.Println("=== Step 1: Simulate Transactions ===")
	tx1 := db.Begin(mvcc.ReadCommitted)
	db.Write(tx1, "key1", "value1")
	fmt.Println("Transaction 1 wrote 'value1'.")

	tx2 := db.Begin(mvcc.ReadCommitted)
	value, err := db.ReadWithIsolation(tx2, "key1", mvcc.ReadCommitted)
	if err != nil {
		fmt.Printf("Transaction 2 failed to read: %v\n", err)
	} else {
		fmt.Printf("Transaction 2 read (Committed): %s\n", value)
	}

	err = db.Commit(tx1)
	if err != nil {
		fmt.Printf("Failed to commit Transaction 1: %v\n", err)
	} else {
		fmt.Println("Transaction 1 committed.")
	}

	// Simulate concurrent transactions
	fmt.Println("=== Step 2: Simulate Concurrent Transactions ===")
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		tx := db.Begin(mvcc.ReadCommitted)
		db.Write(tx, "key1", "value2")
		db.Commit(tx)
		log.Println("Transaction 3 wrote 'value2' and committed.")
	}()

	go func() {
		defer wg.Done()
		tx := db.Begin(mvcc.ReadCommitted)
		db.Write(tx, "key1", "value3")
		db.Commit(tx)
		log.Println("Transaction 4 wrote 'value3' and committed.")
	}()

	wg.Wait()

	// Validate results
	tx3 := db.Begin(mvcc.ReadCommitted)
	value, err = db.ReadWithIsolation(tx3, "key1", mvcc.ReadCommitted)
	if err != nil {
		fmt.Printf("Transaction 5 failed to read: %v\n", err)
	} else {
		fmt.Printf("Transaction 5 read (Committed): %s\n", value)
	}

	fmt.Println("=== Program Completed ===")
}

// Additional modifications will continue across the `pkg/mvcc/` folder and `tests/` files for detailed granularity.
