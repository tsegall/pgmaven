package commands

import (
	"fmt"
	"sort"

	"golang.org/x/exp/maps"
)

type Help struct {
}

func (command *Help) Execute(args ...string) {
	keys := maps.Keys(commandRegistry)
	sort.Strings(keys)
	for i, key := range keys {
		if i != 0 {
			fmt.Print(", ")
		}
		fmt.Print(key)
	}
	fmt.Println()
}
