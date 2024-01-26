package plugins

import (
	"fmt"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type AnalyzeTables struct {
	issues []utils.Issue
}

// DuplicateIndexes reports on redundant indexes.
func (d *AnalyzeTables) Execute(args ...string) {
	d.issues = make([]utils.Issue, 0)

	err, tableNames := dbutils.TableList(100000)
	if err != nil {
		fmt.Printf("ERROR: AnalyzeTables: failed to list tables")
		return
	}

	tableAnalyzer := new(AnalyzeTable)
	for _, tableName := range tableNames {
		tableAnalyzer.Execute(tableName)
		d.issues = append(d.issues, tableAnalyzer.GetIssues()...)
	}
}

func (d *AnalyzeTables) GetIssues() []utils.Issue {
	return d.issues
}
