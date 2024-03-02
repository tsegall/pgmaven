package issues

import (
	"log"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
	"time"
)

type AllIssues struct {
	datasource *dbutils.DataSource
	context    utils.Context
	issues     []utils.Issue
	timing     utils.Timing
}

func (d *AllIssues) Init(context utils.Context, ds *dbutils.DataSource) {
	d.datasource = ds
	d.context = context
}

// Run a set of detection routines.
func (d *AllIssues) Execute(args ...string) {
	startMS := time.Now().UnixMilli()

	routines := []string{"TableIssues", "IndexIssues"}

	for _, element := range routines {
		sub, err := NewDetector(element)
		if err != nil {
			log.Printf("ERROR: AllIssues failed with error: %v\n", err)
			continue
		}
		sub.Init(d.context, d.datasource)
		sub.Execute()
		d.issues = append(d.issues, sub.GetIssues()...)
	}

	d.timing.SetDurationMS(time.Now().UnixMilli() - startMS)
}

func (d *AllIssues) GetIssues() []utils.Issue {
	return d.issues
}

func (d *AllIssues) GetDurationMS() int64 {
	return d.timing.GetDurationMS()
}
