package plugins

import (
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type AllIssues struct {
	datasource *dbutils.DataSource
	issues     []utils.Issue
	durationMS int64
}

func (d *AllIssues) Init(ds *dbutils.DataSource) {
	d.datasource = ds
}

// Run a set of detection routines.
func (d *AllIssues) Execute(args ...string) {

	routines := []string{"AnalyzeTables", "DuplicateIndexes", "SillyIndexes", "UnusedIndexes"}

	// using for loop
	for _, element := range routines {
		sub, _ := NewDetector(d.datasource, element)
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
