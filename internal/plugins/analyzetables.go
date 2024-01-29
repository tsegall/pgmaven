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
func (a *AnalyzeTables) Execute(args ...string) {
	a.issues = make([]utils.Issue, 0)

	err, tableNames := dbutils.TableList(100000)
	if err != nil {
		fmt.Printf("ERROR: AnalyzeTables: failed to list tables")
		return
	}

	tableAnalyzer := new(AnalyzeTable)
	for _, tableName := range tableNames {
		tableAnalyzer.Execute(tableName)
		a.issues = append(a.issues, tableAnalyzer.GetIssues()...)
	}
}

func (a *AnalyzeTables) GetIssues() []utils.Issue {
	return a.issues
}
