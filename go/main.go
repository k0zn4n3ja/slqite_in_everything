// Package main implements a simple SQLite-like database REPL.
//
// It provides a command-line interface for executing SQL-like statements and meta-commands.
package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// don't know about this one
const (
	COLUMN_USERNAME_SIZE = 32
	COLUMN_EMAIL_SIZE    = 255
	PAGE_SIZE            = 4096
	TABLE_MAX_PAGES      = 100
	ROW_SIZE             = 4 + COLUMN_USERNAME_SIZE + COLUMN_EMAIL_SIZE
)

const (
	ROWS_PER_PAGE  = PAGE_SIZE / ROW_SIZE
	TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES
)

// ExecuteResult represents the result of executing a statement
type ExecuteResult int

const (
	ExecuteSuccess ExecuteResult = iota
	ExecuteTableFull
)

type MetaCommandResult int

const (
	MetaCommandSuccess MetaCommandResult = iota
	MetaCommandUnrecognized
)

type PrepareResult int

const (
	PrepareSuccess PrepareResult = iota
	PrepareSyntaxError
	PrepareUnrecognizedStatement
)

type StatementType int

const (
	StatementInsert StatementType = iota
	StatementSelect
)

// Row represents a database row
type Row struct {
	ID uint32
	// TODO are we going to parse this into a string??
	Username [COLUMN_USERNAME_SIZE]byte
	Email    [COLUMN_EMAIL_SIZE]byte
}

// Statement represents a SQL statement
type Statement struct {
	Type        StatementType
	RowToInsert Row // only used by insert statement
}

// Table represents the database table with paged storage
type Table struct {
	NumRows uint32
	// TODO don't quite understand this
	Pages [TABLE_MAX_PAGES][]byte
}

type InputBuffer struct {
	buffer string
}

func newInputBuffer() *InputBuffer {
	return &InputBuffer{
		buffer: "",
	}
}

func newTable() *Table {
	return &Table{
		NumRows: 0,
		Pages:   [TABLE_MAX_PAGES][]byte{},
	}
}

func printPrompt() {
	fmt.Print("db > ")
}

func printRow(row *Row) {
	// Convert byte arrays to strings, trimming null bytes
	username := string(row.Username[:])
	if nullIndex := strings.IndexByte(username, 0); nullIndex >= 0 {
		username = username[:nullIndex]
	}

	email := string(row.Email[:])
	if nullIndex := strings.IndexByte(email, 0); nullIndex >= 0 {
		email = email[:nullIndex]
	}

	fmt.Printf("(%d, %s, %s)\n", row.ID, username, email)
}

func serializeRow(source *Row, destination []byte) {
	binary.LittleEndian.PutUint32(destination[0:4], source.ID)
	copy(destination[4:4+COLUMN_USERNAME_SIZE], source.Username[:])
	copy(destination[4+COLUMN_USERNAME_SIZE:4+COLUMN_USERNAME_SIZE+COLUMN_EMAIL_SIZE], source.Email[:])
}

func deserializeRow(source []byte, destination *Row) {
	destination.ID = binary.LittleEndian.Uint32(source[0:4])
	copy(destination.Username[:], source[4:4+COLUMN_USERNAME_SIZE])
	copy(destination.Email[:], source[4+COLUMN_USERNAME_SIZE:4+COLUMN_USERNAME_SIZE+COLUMN_EMAIL_SIZE])
}

func rowSlot(table *Table, rowNum uint32) []byte {
	pageNum := rowNum / ROWS_PER_PAGE
	if table.Pages[pageNum] == nil {
		// Allocate memory only when we try to access page
		table.Pages[pageNum] = make([]byte, PAGE_SIZE)
	}
	rowOffset := rowNum % ROWS_PER_PAGE
	byteOffset := rowOffset * ROW_SIZE
	return table.Pages[pageNum][byteOffset : byteOffset+ROW_SIZE]
}

// readInput reads a line of input from the scanner into the input buffer.
func readInput(inputBuffer *InputBuffer, scanner *bufio.Scanner) {
	scanner.Scan() // Split function on newline
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

		parts := strings.Fields(inputBuffer.buffer)
		if len(parts) < 4 {
			return PrepareSyntaxError
		}

		id, err := strconv.ParseUint(parts[1], 10, 32)
		if err != nil {
			return PrepareSyntaxError
		}
		statement.RowToInsert.ID = uint32(id)

		username := parts[2]
		if len(username) >= COLUMN_USERNAME_SIZE {
			username = username[:COLUMN_USERNAME_SIZE-1]
		}

		// These are stored as bytes so we want to copy the raw bytes here
		copy(statement.RowToInsert.Username[:], username)

		email := parts[3]
		if len(email) >= COLUMN_EMAIL_SIZE {
			email = email[:COLUMN_EMAIL_SIZE-1]
		}

		// These are stored as bytes so we want to copy the raw bytes here
		copy(statement.RowToInsert.Email[:], email)

		return PrepareSuccess
	}

	if inputBuffer.buffer == "select" {
		statement.Type = StatementSelect
		return PrepareSuccess
	}

	return PrepareUnrecognizedStatement
}

func executeInsert(statement *Statement, table *Table) ExecuteResult {
	if table.NumRows >= TABLE_MAX_ROWS {
		return ExecuteTableFull
	}

	rowToInsert := &statement.RowToInsert
	serializeRow(rowToInsert, rowSlot(table, table.NumRows))
	table.NumRows++

	return ExecuteSuccess
}

func executeSelect(statement *Statement, table *Table) ExecuteResult {
	var row Row
	for i := uint32(0); i < table.NumRows; i++ {
		deserializeRow(rowSlot(table, i), &row)
		printRow(&row)
	}
	return ExecuteSuccess
}

func executeStatement(statement *Statement, table *Table) ExecuteResult {
	switch statement.Type {
	case StatementInsert:
		return executeInsert(statement, table)
	case StatementSelect:
		return executeSelect(statement, table)
	default:
		return ExecuteSuccess
	}
}

func main() {
	table := newTable()
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
		case PrepareSyntaxError:
			fmt.Println("Syntax error. Could not parse statement.")
			continue
		case PrepareUnrecognizedStatement:
			fmt.Printf("Unrecognized keyword at start of '%s'.\n", inputBuffer.buffer)
			continue
		}

		switch executeStatement(statement, table) {
		case ExecuteSuccess:
			fmt.Println("Executed.")
		case ExecuteTableFull:
			fmt.Println("Error: Table full.")
		}
	}
}
