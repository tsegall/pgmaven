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
	issues []utils.Issue
	series []IntegerTimeSeries
}

// DuplicateIndexes reports on redundant indexes.
func (a *AnalyzeTable) Execute(args ...string) {
	a.issues = make([]utils.Issue, 0)

	query := fmt.Sprintf(`select n_live_tup, insert_dt from pgmaven_pg_stat_user_tables where relname = '%s' and last_analyze is not null order by insert_dt`, args[0])

	a.series = make([]IntegerTimeSeries, 0)

	err := dbutils.ExecuteQueryRows(query, nil, analyzeTableProcessor, a)
	if err != nil {
		log.Printf("ERROR: AnalyzeTable: table '%s' failed with error: %v\n", args[0], err)
	}

	if len(a.series) < 2 {
		fmt.Printf("ERROR: AnalyzeTable: table '%s', insufficient snapshots to analyze\n", args[0])
		return
	}

	start := a.series[0].when
	end := a.series[len(a.series)-1].when

	timeDiff := end.Sub(start).Milliseconds() / 1000
	countDiff := a.series[len(a.series)-1].value - a.series[0].value

	const daySeconds = 24 * 60 * 60

	if timeDiff < daySeconds/2 {
		fmt.Printf("ERROR: AnalyzeTable: insufficient data captured by snapshots (%d seconds)\n", timeDiff)
		return
	}

	days := float32(timeDiff) / daySeconds
	rowsPerDay := float32(countDiff) / days
	dailyPercent := 100 * rowsPerDay / float32(a.series[0].value)

	// fmt.Printf("Analysis period: %s - %s (%ds): %d rows, rows per day: %f, daily percent: %f\n", start, end, timeDiff, countDiff, rowsPerDay, dailyPercent)

	// for _, elt := range series {
	// 	fmt.Printf("%s: %d\n", elt.when, elt.value)
	// }

	detail := fmt.Sprintf("Table: %s, current rows: %d, is growing at %.2f%% per day\n", args[0], a.series[len(a.series)-1].value, dailyPercent)

	if dailyPercent > 0.5 {
		a.issues = append(a.issues, utils.Issue{IssueType: "TableGrowth", Detail: detail, Solution: "REVIEW table - consider partitioning and/or pruning\n"})
	}
}

// AnalyzeTableProcessor is invoked for every row of the Analyze Table Query.
// The Query returns a row with the following format (n_live_tup, insert_dt)
func analyzeTableProcessor(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}, self any) {
	rows := (*values[0].(*interface{})).(int64)
	insert_dt := (*values[1].(*interface{})).(time.Time)
	a := self.(*AnalyzeTable)

	a.series = append(a.series, IntegerTimeSeries{when: insert_dt, value: rows})
}

func (a *AnalyzeTable) GetIssues() []utils.Issue {
	return a.issues
}
