package utils

import (
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	SIZE_GB = 1024 * 1024 * 1024
	SIZE_MB = 1024 * 1024
	SIZE_KB = 1024
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

var shorthand = [...]string{
	"1", "2", "4", "8", "16", "32", "64", "128", "256", "512",
	"1kB", "2kB", "4kB", "8kB", "16kB", "32kB", "64kB", "128kB", "256kB", "512kB",
	"1MB", "2MB", "4MB", "8MB", "16MB", "32MB", "64MB", "128MB", "256MB", "512MB",
	"1GB", "2GB", "4GB", "8GB", "16GB", "32GB", "64GB", "128GB", "256GB", "512GB",
}

func PrettyPrint(v int64) string {
	if v == 0 {
		return "0"
	}

	msb := MSB(v)

	// If we are a power of two we are done
	if v == 1<<msb {
		return shorthand[MSB(v)]
	}

	lsb := LSB(v)
	if lsb < 10 {
		return strconv.FormatInt(v, 10)
	} else if lsb < 20 {
		return strconv.FormatInt(v>>10, 10) + "kB"
	} else if lsb < 30 {
		return strconv.FormatInt(v>>20, 10) + "MB"
	}

	return strconv.FormatInt(v>>30, 10) + "GB"
}

func CleartoKB(v int64) int64 {
	return (v >> 10) << 10
}

func CleartoMB(v int64) int64 {
	return (v >> 20) << 20
}

func CleartoGB(v int64) int64 {
	return (v >> 30) << 30
}

func PgUnitsToBytes(v int64, units string) int64 {
	var multiplier int64
	if units == "B" {
		multiplier = 1
	} else if units == "kB" {
		multiplier = 1024
	} else if units == "8kB" {
		multiplier = 8 * 1024
	} else if units == "MB" {
		multiplier = 1024 * 1024
	}

	return v * multiplier
}

func LSB(v int64) (r int) {
	if v == 0 {
		return -1
	}

	for (v & 1) == 0 {
		v >>= 1
		r++
	}

	return
}

func MSB(v int64) (r int) {
	if v == 0 {
		return -1
	}

	for (v >> 1) != 0 {
		v = v >> 1
		r++
	}

	return
}
