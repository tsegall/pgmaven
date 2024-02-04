package plugins

import (
	"fmt"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
	"time"
)

type QueryLoad struct {
	datasource *dbutils.DataSource
	context    utils.Context
	issues     []utils.Issue
	durationMS int64
}

func (d *QueryLoad) Init(context utils.Context, ds *dbutils.DataSource) {
	d.datasource = ds
	d.context = context
}

func (d *QueryLoad) Execute(args ...string) {
	startMS := time.Now().UnixMilli()
	d.issues = make([]utils.Issue, 0)

	if d.context.Verbose {
		fmt.Printf("Analyzing load for duration: %v\n", d.context.Duration)
	}

	d.durationMS = time.Now().UnixMilli() - startMS
}

func (d *QueryLoad) GetIssues() []utils.Issue {
	return d.issues
}

func (d *QueryLoad) GetDurationMS() int64 {
	return d.durationMS
}
