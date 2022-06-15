//go:generate go run generate.go

package main

import (
	"fmt"
	"os"

	"github.com/deephaven/deephaven-core/go-client/examples"
)

var exampleMap = map[string]func(){
	"ExampleFetchTable":  examples.ExampleFetchTable,
	"ExampleRunScript":   examples.ExampleRunScript,
	"ExampleImportTable": examples.ExampleImportTable,
	"ExampleQuery":       examples.ExampleQuery,
}

func printUsage() {
	fmt.Println("Usage (binary): go-client <example name>")
	fmt.Println("Usage (source): go run . <example name>")
	fmt.Println("Valid example names:")
	for name := range exampleMap {
		fmt.Println("\t", name)
	}
}

func main() {
	if len(os.Args) <= 1 {
		fmt.Println("Not enough arguments")
		printUsage()
		return
	}
	if len(os.Args) >= 3 {
		fmt.Println("Too many arguments!")
		printUsage()
		return
	}

	if example, ok := exampleMap[os.Args[1]]; ok {
		example()
	} else {
		fmt.Printf("Invalid example name `%s`\n", os.Args[1])
		return
	}
}
