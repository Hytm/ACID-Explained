package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
)

func main() {
	pr := false
	ws := false
	flag.BoolVar(&pr, "phantom", false, "Run phantom read test")
	flag.BoolVar(&ws, "write", false, "Run write skew test")
	flag.Parse()

	err := godotenv.Load(".env")
	if err != nil {
		log.Fatal("Error loading .env file:", err)
	}

	if !pr && !ws {
		usageAndExit()
		return
	}

	if pr {
		fmt.Printf("Testing Phantom Read\n\n*********************************\n\n")
		phantom_read()
	}
	if ws {
		fmt.Printf("\n\n*********************************\n\nTesting Write Skew\n\n*********************************\n\n")
		writeSkew()
	}
}

func usageAndExit() {
	fmt.Printf("Usage: go run *.go or binary name [options]\n\n")
	fmt.Printf("Options:\n")
	fmt.Printf("  -phantom\tRun phantom read test\n")
	fmt.Printf("  -write\tRun write skew test\n")
	os.Exit(0)
}
