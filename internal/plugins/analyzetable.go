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

var series []IntegerTimeSeries

type AnalyzeTable struct {
	issues []utils.Issue
}

// DuplicateIndexes reports on redundant indexes.
func (d *AnalyzeTable) Execute(args ...interface{}) {
	d.issues = make([]utils.Issue, 0)

	query := fmt.Sprintf(`select n_live_tup, insert_dt from pgmaven_pg_stat_user_tables where relname = '%s' and last_analyze is not null order by insert_dt`, args[0])

	series = make([]IntegerTimeSeries, 0)

	err := dbutils.ExecuteQueryRows(query, AnalyzeTableProcessor)
	if err != nil {
		log.Printf("ERROR: AnalyzeTable: table '%s' failed with error: %v\n", args[0], err)
	}

	if len(series) < 2 {
		fmt.Printf("ERROR: AnalyzeTable: table '%s', insufficient snapshots to analyze\n", args[0])
		return
	}

	start := series[0].when
	end := series[len(series)-1].when

	timeDiff := end.Sub(start).Milliseconds() / 1000
	countDiff := series[len(series)-1].value - series[0].value

	const daySeconds = 24 * 60 * 60

	if timeDiff < daySeconds/2 {
		fmt.Printf("ERROR: AnalyzeTable: insufficient data captured by snapshots (%d seconds)\n", timeDiff)
		return
	}

	days := float32(timeDiff) / daySeconds
	rowsPerDay := float32(countDiff) / days
	dailyPercent := 100 * rowsPerDay / float32(series[0].value)

	// fmt.Printf("Analysis period: %s - %s (%ds): %d rows, rows per day: %f, daily percent: %f\n", start, end, timeDiff, countDiff, rowsPerDay, dailyPercent)

	// for _, elt := range series {
	// 	fmt.Printf("%s: %d\n", elt.when, elt.value)
	// }

	detail := fmt.Sprintf("Table: %s is growing at %.2f%% per day\n", args[0], dailyPercent)

	if dailyPercent > 0.5 {
		d.issues = append(d.issues, utils.Issue{IssueType: "TableGrowth", Detail: detail, Solution: "REVIEW table - consider partitioning and/or pruning\n"})
	}
}

// AnalyzeTableProcessor is invoked for every row of the Analyze Table Query.
// The Query returns a row with the following format (n_live_tup, insert_dt)
func AnalyzeTableProcessor(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}) {
	rows := (*values[0].(*interface{})).(int64)
	insert_dt := (*values[1].(*interface{})).(time.Time)

	series = append(series, IntegerTimeSeries{when: insert_dt, value: rows})
}

func (d *AnalyzeTable) GetIssues() []utils.Issue {
	return d.issues
}
