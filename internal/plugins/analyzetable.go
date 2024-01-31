package plugins

import (
	"database/sql"
	"fmt"
	"log"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
	"time"
)

type IntegerTimeSeries struct {
	when  time.Time
	value int64
}

type AnalyzeTable struct {
	datasource *dbutils.DataSource
	issues     []utils.Issue
	series     []IntegerTimeSeries
	durationMS int64
}

func (d *AnalyzeTable) Init(ds *dbutils.DataSource) {
	d.datasource = ds
}

func (d *AnalyzeTable) Execute(args ...string) {
	startMS := time.Now().UnixMilli()
	d.issues = make([]utils.Issue, 0)

	query := fmt.Sprintf(`select n_live_tup, insert_dt from pgmaven_pg_stat_user_tables where relname = '%s' and last_analyze is not null order by insert_dt`, args[0])

	d.series = make([]IntegerTimeSeries, 0)

	err := d.datasource.ExecuteQueryRows(query, nil, analyzeTableProcessor, d)
	if err != nil {
		log.Printf("ERROR: AnalyzeTable: table '%s' failed with error: %v\n", args[0], err)
	}

	if len(d.series) < 2 {
		fmt.Printf("ERROR: AnalyzeTable: table '%s', insufficient snapshots to analyze\n", args[0])
		return
	}

	start := d.series[0].when
	end := d.series[len(d.series)-1].when

	timeDiff := end.Sub(start).Milliseconds() / 1000
	countDiff := d.series[len(d.series)-1].value - d.series[0].value

	const daySeconds = 24 * 60 * 60

	if timeDiff < daySeconds/2 {
		fmt.Printf("ERROR: AnalyzeTable: insufficient data captured by snapshots (%d seconds)\n", timeDiff)
		return
	}

	days := float32(timeDiff) / daySeconds
	rowsPerDay := float32(countDiff) / days
	dailyPercent := 100 * rowsPerDay / float32(d.series[0].value)

	// fmt.Printf("Analysis period: %s - %s (%ds): %d rows, rows per day: %f, daily percent: %f\n", start, end, timeDiff, countDiff, rowsPerDay, dailyPercent)

	// for _, elt := range series {
	// 	fmt.Printf("%s: %d\n", elt.when, elt.value)
	// }

	detail := fmt.Sprintf("Table: %s, current rows: %d, is growing at %.2f%% per day\n", args[0], d.series[len(d.series)-1].value, dailyPercent)

	if dailyPercent > 0.5 {
		d.issues = append(d.issues, utils.Issue{IssueType: "TableGrowth", Detail: detail, Solution: "REVIEW table - consider partitioning and/or pruning\n"})
	}

	d.durationMS = time.Now().UnixMilli() - startMS
}

// AnalyzeTableProcessor is invoked for every row of the Analyze Table Query.
// The Query returns a row with the following format (n_live_tup, insert_dt)
func analyzeTableProcessor(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}, self any) {
	rows := (*values[0].(*interface{})).(int64)
	insert_dt := (*values[1].(*interface{})).(time.Time)
	d := self.(*AnalyzeTable)

	d.series = append(d.series, IntegerTimeSeries{when: insert_dt, value: rows})
}

func (d *AnalyzeTable) GetIssues() []utils.Issue {
	return d.issues
}

func (d *AnalyzeTable) GetDurationMS() int64 {
	return d.durationMS
}
