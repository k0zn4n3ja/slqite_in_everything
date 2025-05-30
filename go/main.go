// Package main implements a simple SQLite-like database REPL.
//
// It provides a command-line interface for executing SQL-like statements and meta-commands.
package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type MetaCommandResult int

const (
	MetaCommandSuccess MetaCommandResult = iota
	MetaCommandUnrecognized
)

type PrepareResult int

const (
	PrepareSuccess PrepareResult = iota
	PrepareUnrecognizedStatement
)

type StatementType int

const (
	StatementInsert StatementType = iota
	StatementSelect
)

type Statement struct {
	Type StatementType
}

type InputBuffer struct {
	buffer string
}

func newInputBuffer() *InputBuffer {
	return &InputBuffer{
		buffer: "",
	}
}

func printPrompt() {
	fmt.Print("db > ")
}

// readInput reads a line of input from the scanner into the input buffer.
func readInput(inputBuffer *InputBuffer, scanner *bufio.Scanner) {
	scanner.Scan()
	inputBuffer.buffer = scanner.Text()
}

// parseAndExecuteMeta parses and executes meta commands.
//
// Returns MetaCommandSuccess if the command was recognized and executed,
//
// or MetaCommandUnrecognized if the command was not recognized.
func parseAndExecuteMeta(inputBuffer *InputBuffer) MetaCommandResult {
	if inputBuffer.buffer == ".exit" {
		os.Exit(0)
	}
	return MetaCommandUnrecognized
}

// isMetaCommand checks if the input buffer contains a meta command.
//
// Meta commands start with a dot (.).
func isMetaCommand(inputBuffer *InputBuffer) bool {
	return strings.HasPrefix(inputBuffer.buffer, ".")
}

// prepareStatement prepares a SQL statement from the input buffer.
//
// Returns PrepareSuccess if the statement was recognized,
// or PrepareUnrecognizedStatement if the statement was not recognized.
func prepareStatement(inputBuffer *InputBuffer, statement *Statement) PrepareResult {
	if strings.HasPrefix(inputBuffer.buffer, "insert") {
		statement.Type = StatementInsert
		return PrepareSuccess
	}
	if strings.HasPrefix(inputBuffer.buffer, "select") {
		statement.Type = StatementSelect
		return PrepareSuccess
	}
	return PrepareUnrecognizedStatement
}

func executeStatement(statement *Statement) {
	switch statement.Type {
	case StatementInsert:
		fmt.Println("insert statement logic here.")
	case StatementSelect:
		fmt.Println("select statement logic here.")
	}
}

func main() {
	inputBuffer := newInputBuffer()
	scanner := bufio.NewScanner(os.Stdin)

	for {
		printPrompt()
		readInput(inputBuffer, scanner)

		if isMetaCommand(inputBuffer) {
			switch parseAndExecuteMeta(inputBuffer) {
			case MetaCommandSuccess:
				continue
			case MetaCommandUnrecognized:
				fmt.Printf("Unrecognized command '%s'\n", inputBuffer.buffer)
				continue
			}
		}

		statement := &Statement{}
		switch prepareStatement(inputBuffer, statement) {
		case PrepareSuccess:
			// this breaks the switch not the loop
			break
		case PrepareUnrecognizedStatement:
			fmt.Printf("Unrecognized keyword at start of '%s'.\n", inputBuffer.buffer)
			continue
		}

		executeStatement(statement)
		fmt.Println("Executed.")
	}
}
