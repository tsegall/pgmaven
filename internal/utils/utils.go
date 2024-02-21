package utils

import (
	"log"
	"os"
	"strings"
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

func QuoteAlways(s string) string {
	s = strings.ReplaceAll(s, "\"", "\"\"")

	return "\"" + s + "\""
}

func RemoveBlankLines(s string) string {
	return strings.ReplaceAll(s, "\n\n", "\n")
}

func QuoteIfNeeded(s string) string {
	commaIndex := strings.Index(s, ",")
	tabIndex := strings.Index(s, "\t")
	quoteIndex := strings.Index(s, "\"")

	if commaIndex == -1 && tabIndex == -1 && quoteIndex == -1 {
		return s
	}

	if quoteIndex != -1 {
		s = strings.ReplaceAll(s, "\"", "\"\"")
	}

	return "\"" + s + "\""
}
