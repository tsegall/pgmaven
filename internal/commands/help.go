package commands

import (
	"fmt"
	"pgmaven/internal/dbutils"
	"sort"

	"golang.org/x/exp/maps"
)

type Help struct {
}

func (h *Help) Init(ds *dbutils.DataSource) {
}

func (h *Help) Execute(args ...string) {
	keys := maps.Keys(commandRegistry)
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Printf("%s - %s\n", key, commandRegistry[key].HelpText)
	}
}
