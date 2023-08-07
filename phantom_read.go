package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

func phantom_read() {
	fmt.Println("Testing with PostgreSQL")
	// Establish a connection to the PostgreSQL database
	runTestPR(os.Getenv("PG_URL"))

	fmt.Printf("\n\nTesting with CockroachDB")
	// Establish a connection to the PostgreSQL database
	runTestPR(os.Getenv("CRDB_URL"))
}

func runTestPR(URL string) {
	db, err := sql.Open("postgres", URL)
	if err != nil {
		log.Fatal("Error connecting to the database:", err)
	}

	fmt.Printf("\npreparing tables")
	prepTestPR(db)

	fmt.Printf("\nTesting the isolation")
	run(db)

	db.Close()
}

func prepTestPR(db *sql.DB) {
	_, err := db.Exec("DROP TABLE IF EXISTS accounts")
	if err != nil {
		log.Fatal("Error dropping table:", err)
	}

	// Create a table to demonstrate phantom reads
	_, err = db.Exec("CREATE TABLE accounts (id SERIAL, price INT, isBilled BOOLEAN DEFAULT FALSE)")
	if err != nil {
		log.Fatal("Error creating table:", err)
	}

	// Insert some initial data
	_, err = db.Exec("INSERT INTO accounts (id, price) VALUES (1, 10), (2, 20), (3, 10), (4, 20), (5, 10), (6, 20), (7, 10)")
	if err != nil {
		log.Fatal("Error inserting initial data:", err)
	}
}

func run(db *sql.DB) {
	// Create a wait group to synchronize the goroutines
	var wg sync.WaitGroup
	wg.Add(2)

	// Start two goroutines to perform phantom reads
	go phantomReadBilling(db, &wg)
	go changeAccountStatus(db, &wg)

	// Wait for the goroutines to finish
	wg.Wait()
}

func phantomReadBilling(db *sql.DB, wg *sync.WaitGroup) {
	defer wg.Done()

	// Begin a transaction
	tx, err := db.Begin()
	if err != nil {
		log.Fatal("Error beginning transaction:", err)
	}

	// Retrieve the initial set of products
	rows, err := tx.Query("SELECT id, price FROM accounts WHERE isBilled = FALSE")
	if err != nil {
		log.Fatal("Error retrieving accounts:", err)
	}
	defer rows.Close()

	// Print the initial set of products
	fmt.Printf("\nInitial billing to be done:\n")
	total := 0
	for rows.Next() {
		var id int
		var price int
		err = rows.Scan(&id, &price)
		if err != nil {
			log.Fatal("Error scanning row:", err)
		}
		fmt.Printf("ID: %d, Price: %d\n", id, price)
		total += price
	}
	fmt.Printf("\nTotal: %d\n", total)

	// Simulate some processing time
	time.Sleep(2 * time.Second)

	// Retrieve the updated set of products
	rows, err = tx.Query("SELECT id, price FROM accounts WHERE isBilled = FALSE")
	if err != nil {
		log.Fatal("Error retrieving accounts:", err)
	}
	defer rows.Close()

	// Print the updated set of products
	billed := 0
	fmt.Printf("\nBilling:\n")
	for rows.Next() {
		var id int
		var price int
		err = rows.Scan(&id, &price)
		if err != nil {
			log.Fatal("Error scanning row:", err)
		}
		fmt.Printf("ID: %d billed for: %d\n", id, price)
		billed += price
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		log.Fatal("Error committing transaction:", err)
	}
	fmt.Printf("\nTotal billed: %d\n", billed)
}

func changeAccountStatus(db *sql.DB, wg *sync.WaitGroup) {
	defer wg.Done()

	// delay second transaction
	time.Sleep(500 * time.Millisecond)

	// Begin a transaction
	tx, err := db.Begin()
	if err != nil {
		log.Fatal("Error beginning transaction:", err)
	}

	// Insert a new product
	_, err = tx.Exec("UPDATE accounts SET isBilled = TRUE WHERE price = 20")
	if err != nil {
		log.Fatal("Error inserting product:", err)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		log.Fatal("Error committing transaction:", err)
	}
}
