package plugins

import (
	"fmt"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
	"time"
)

type AnalyzeTables struct {
	datasource *dbutils.DataSource
	issues     []utils.Issue
	durationMS int64
}

func (d *AnalyzeTables) Init(ds *dbutils.DataSource) {
	d.datasource = ds
}

func (d *AnalyzeTables) Execute(args ...string) {
	startMS := time.Now().UnixMilli()
	d.issues = make([]utils.Issue, 0)

	tableNames, err := d.datasource.TableList(100000)
	if err != nil {
		fmt.Printf("ERROR: AnalyzeTables: failed to list tables")
		return
	}

	tableAnalyzer := new(AnalyzeTable)
	tableAnalyzer.Init(d.datasource)
	for _, tableName := range tableNames {
		tableAnalyzer.Execute(tableName)
		d.issues = append(d.issues, tableAnalyzer.GetIssues()...)
	}

	d.durationMS = time.Now().UnixMilli() - startMS
}

func (d *AnalyzeTables) GetIssues() []utils.Issue {
	return d.issues
}

func (d *AnalyzeTables) GetDurationMS() int64 {
	return d.durationMS
}
