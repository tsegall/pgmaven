package plugins

import (
	"fmt"
	"pgmaven/internal/utils"
	"sort"

	"golang.org/x/exp/maps"
)

type Help struct {
}

func (d *Help) Execute(args ...string) {
	keys := maps.Keys(detectorRegistry)
	sort.Strings(keys)
	for i, key := range keys {
		if i != 0 {
			fmt.Print(", ")
		}
		fmt.Print(key)
	}
	fmt.Println()
}

func (d *Help) GetIssues() []utils.Issue {
	return nil
}
