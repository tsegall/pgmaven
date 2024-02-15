package utils

import (
	"fmt"
)

type IssueSeverity int32

func (i IssueSeverity) String() string {
	return [...]string{"HIGH", "MEDIUM", "LOW"}[i]
}

const (
	High IssueSeverity = iota
	Medium
	Low
)

type Issue struct {
	IssueType string
	Target    string
	Severity  IssueSeverity
	Detail    string
	Solution  string
}

func (i *Issue) Dump() {
	fmt.Printf("ISSUE: %s\n", i.IssueType)
	fmt.Printf("SEVERITY: %s\n", i.Severity)
	fmt.Printf("TARGET: %s\n", i.Target)
	fmt.Printf("DETAIL:\n%s", indent(i.Detail))
	fmt.Printf("SUGGESTION:\n%s", indent(i.Solution))
}

func indent(s string) (ret string) {
	ret = "\t"
	for i, c := range s {
		ret += string(c)
		if c == '\n' && i != len(s)-1 {
			ret += "\t"
		}
	}

	return
}
