package main

import (
	"bufio"
	"fmt"
	"os"
)

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

func readInput(inputBuffer *InputBuffer, scanner *bufio.Scanner) {
	scanner.Scan()
	inputBuffer.buffer = scanner.Text()
}

func main() {
	inputBuffer := newInputBuffer()
	scanner := bufio.NewScanner(os.Stdin)

	for {
		printPrompt()
		readInput(inputBuffer, scanner)

		// TODO what is this
		if inputBuffer.buffer == ".exit" {
			os.Exit(0)
		} else {
			fmt.Printf("Unrecognized command '%s'.\n", inputBuffer.buffer)
		}
	}
}
