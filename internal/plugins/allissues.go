package plugins

import (
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type AllIssues struct {
	datasource *dbutils.DataSource
	context    utils.Context
	issues     []utils.Issue
	durationMS int64
}

func (d *AllIssues) Init(context utils.Context, ds *dbutils.DataSource) {
	d.datasource = ds
	d.context = context
}

// Run a set of detection routines.
func (d *AllIssues) Execute(args ...string) {

	routines := []string{"AnalyzeTables", "DuplicateIndexes", "SillyIndexes", "UnusedIndexes"}

	for _, element := range routines {
		sub, _ := NewDetector(element)
		sub.Init(d.context, d.datasource)
		sub.Execute()
		d.issues = append(d.issues, sub.GetIssues()...)
	}
}

func (d *AllIssues) GetIssues() []utils.Issue {
	return d.issues
}

func (d *AllIssues) GetDurationMS() int64 {
	return d.durationMS
}
