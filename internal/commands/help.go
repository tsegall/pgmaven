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
	for _, key := range keys {
		fmt.Printf("%s - %s\n", key, commandRegistry[key].HelpText)
	}
}
