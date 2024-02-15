package utils

import (
	"log"
	"os"
)

// If the first character is a '!' then assume what follows is a file containing the text
func OptionallyFromFile(args ...string) string {
	if args[0][0] != '!' {
		return args[0]
	}
	buffer, err := os.ReadFile(args[0][1:])
	if err != nil {
		log.Fatalf("ERROR: Failed to read file '%s', error: %v\n", args[0][1:], err)
	}

	return string(buffer)
}
