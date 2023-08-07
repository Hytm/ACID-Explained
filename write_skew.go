package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

func writeSkew() {
	fmt.Println("Testing with PostgreSQL")
	// Establish a connection to the PostgreSQL database
	runTestWS(os.Getenv("PG_URL"))

	fmt.Printf("\n\nTesting with CockroachDB")
	// Establish a connection to the PostgreSQL database
	runTestWS(os.Getenv("CRDB_URL"))
}

func runTestWS(URL string) {
	db, err := sql.Open("postgres", URL)
	if err != nil {
		log.Fatal("Error connecting to the database:", err)
	}

	fmt.Printf("\npreparing tables")
	prepTestWS(db)

	update(db)

	db.Close()
}

func prepTestWS(db *sql.DB) {
	_, err := db.Exec("DROP TABLE IF EXISTS availability")
	if err != nil {
		log.Fatal("Error dropping table:", err)
	}

	// Create a table to demonstrate write skew
	_, err = db.Exec("CREATE TABLE availability (id SERIAL, name VARCHAR(255), isAvailable INT)")
	if err != nil {
		log.Fatal("Error creating table:", err)
	}

	// Insert initial data
	_, err = db.Query("INSERT INTO availability (id, name, isAvailable) VALUES (1, 'Doctor 1', 1), (2, 'Doctor 2', 0), (3, 'Doctor 3', 0), (4, 'Doctor 4', 1)")
	if err != nil {
		log.Fatal("Error inserting initial data:", err)
	}

	// Retrieve and print the availability
	var name string
	var available bool
	rows, err := db.Query("SELECT name, isAvailable FROM availability")
	if err != nil {
		log.Println("Error retrieving final availability:", err)
	}

	for rows.Next() {
		err := rows.Scan(&name, &available)
		if err != nil {
			log.Println("Error reading availability:", err)
		}
		if available {
			fmt.Printf("\n%s is available", name)
		} else {
			fmt.Printf("\n%s is unavailable", name)
		}
	}
}

func update(db *sql.DB) {
	// Create a wait group to synchronize the goroutines
	var wg sync.WaitGroup
	wg.Add(2)

	// Start two goroutines to update the availability concurrently
	go updateAvailability(db, 1, &wg)
	go updateAvailability(db, 4, &wg)

	// Wait for the goroutines to finish
	wg.Wait()

	var finalAvailability int
	err := db.QueryRow("SELECT SUM(isAvailable) FROM availability").Scan(&finalAvailability)
	if err != nil {
		log.Println("Error retrieving availability:", err)
	}
	fmt.Printf("\n\nFinal availability: %d\n", finalAvailability)
}

func updateAvailability(db *sql.DB, id int, wg *sync.WaitGroup) {
	defer wg.Done()

	commitNeeded := true
	fmt.Printf("\nUpdating doctor %d as unavailable\n", id)
	// Begin a transaction
	tx, err := db.Begin()
	if err != nil {
		log.Println("Error beginning transaction:", err)
	}

	// Retrieve the current availability
	var currentAvailability int
	err = tx.QueryRow("SELECT SUM(isAvailable) FROM availability").Scan(&currentAvailability)
	if err != nil {
		log.Println("Error retrieving availability:", err)
	}

	// Simulate some processing time
	time.Sleep(500 * time.Millisecond)

	// Check if the current availability meets the condition for update
	if currentAvailability > 1 {
		// Update the availability of the product
		_, err = tx.Exec("UPDATE availability SET isAvailable = 0 WHERE id = $1", id)
		if err != nil {
			if strings.Contains(err.Error(), "pq: restart transaction") {
				fmt.Printf("\nTransaction needs to restart for doctor %d as total availability changed", id)
				tx.Rollback()
				return
			}
			log.Println("Error updating availability:", err)
			tx.Rollback()
			return
		}
	}

	// Commit the transaction
	if commitNeeded {
		err = tx.Commit()
		if err != nil {
			if strings.Contains(err.Error(), "pq: restart transaction") {
				fmt.Printf("\nTransaction needs to restart for doctor %d as total availability changed", id)
				tx.Rollback()
				return
			}
			log.Println("Error committing transaction:", err)
		}
	}
}
