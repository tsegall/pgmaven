package plugins

import (
	"fmt"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
	"sort"

	"golang.org/x/exp/maps"
)

type Help struct {
	durationMS int64
}

func (d *Help) Init(ds *dbutils.DataSource) {
}

func (d *Help) Execute(args ...string) {
	keys := maps.Keys(detectorRegistry)
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Printf("%s - %s\n", key, detectorRegistry[key].HelpText)
	}
}

func (d *Help) GetIssues() []utils.Issue {
	return nil
}

func (d *Help) GetDurationMS() int64 {
	return d.durationMS
}
