package utils

import (
	"fmt"
)

type Issue struct {
	IssueType string
	Detail    string
	Solution  string
}

func (i *Issue) Dump() {
	fmt.Printf("ISSUE: %s\n", i.IssueType)
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
