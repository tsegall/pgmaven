package utils

import "fmt"

func RecordIssue(issue string, detail string, solution string) {
	fmt.Printf("ISSUE: %s\n", issue)
	fmt.Printf("DETAIL:\n%s", detail)
	fmt.Printf("SOLUTION:\n%s", solution)
}
