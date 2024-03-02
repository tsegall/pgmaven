package issues

import (
	"database/sql"
	"fmt"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
	"time"
)

type TableIssues struct {
	datasource    *dbutils.DataSource
	context       utils.Context
	issues        []utils.Issue
	timing        utils.Timing
	specificIssue string
}

const (
	tableGrowthThreshold = 0.5
	largeTableThreshold  = 10000000
	minTableReport       = 100000
)

func (d *TableIssues) Init(context utils.Context, ds *dbutils.DataSource) {
	d.datasource = ds
	d.context = context
}

// Search for table-related issues.  Optional arg if provided will constrain to only looking for specific issue.
func (d *TableIssues) Execute(args ...string) {
	startMS := time.Now().UnixMilli()
	d.issues = make([]utils.Issue, 0)

	if len(args) != 0 {
		d.specificIssue = args[0]
	}

	query := `
select relname, min(n_live_tup), max(n_live_tup), min(insert_dt), max(insert_dt), max(n_tup_upd + n_tup_del + n_tup_hot_upd)
	from pgmaven_pg_stat_user_tables
	where last_analyze is not null
	and relname not like 'pgmaven%%'
	group by relname`

	err := d.datasource.ExecuteQueryRows(query, nil, tableIssuesProcessor, d)

	if err != nil {
		fmt.Printf("ERROR: Database: %s, TableIssues: failed to list tables, error: %v\n", d.datasource.GetDBName(), err)
		return
	}

	d.timing.SetDurationMS(time.Now().UnixMilli() - startMS)
}

func (d *TableIssues) isIssueEnabled(issue string) bool {
	if d.specificIssue == "" {
		return true
	}

	return issue == d.specificIssue
}

func tableIssuesProcessor(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}, self any) {
	d := self.(*TableIssues)
	tableName := string((*values[0].(*interface{})).([]uint8))
	minRows := (*values[1].(*interface{})).(int64)
	maxRows := (*values[2].(*interface{})).(int64)
	minInsertDt := (*values[3].(*interface{})).(time.Time)
	maxInsertDt := (*values[4].(*interface{})).(time.Time)
	changes := (*values[5].(*interface{})).(int64)
	timeDiff := maxInsertDt.Sub(minInsertDt).Milliseconds() / 1000
	countDiff := maxRows - minRows

	const daySeconds = 24 * 60 * 60

	if timeDiff < daySeconds/2 {
		fmt.Printf("WARNING: Database: %s, TableIssues: Table: %s, insufficient data captured by snapshots (%d seconds)\n", d.datasource.GetDBName(), tableName, timeDiff)
		return
	}

	days := float32(timeDiff) / daySeconds
	rowsPerDay := float32(countDiff) / days
	dailyPercent := 100 * rowsPerDay / float32(maxRows)

	if d.isIssueEnabled("TableGrowth") && maxRows > minTableReport && dailyPercent > tableGrowthThreshold {
		detail := fmt.Sprintf("Table: %s, current rows: %d, is growing at %.2f%% per day\n%s", tableName, maxRows, dailyPercent, d.getUnusedIndexes(tableName))
		d.issues = append(d.issues, utils.Issue{IssueType: "TableGrowth", Target: tableName, Detail: detail, Severity: utils.Medium, Solution: "REVIEW table - consider partitioning and/or pruning\n"})
	}

	if d.isIssueEnabled("TableSizeLarge") && maxRows > largeTableThreshold {
		isPartitionedQuery := `
SELECT count(*)
	FROM   pg_catalog.pg_inherits
	WHERE  inhparent = $1::regclass`
		partitionCount, _ := d.datasource.ExecuteQueryRow(isPartitionedQuery, []any{tableName})
		if partitionCount.(int64) == 0 {
			detail := fmt.Sprintf("Table: %s, current rows: %.2fM, insert only: %t, is large and not partitioned\n%s", tableName, float32(maxRows)/10000000.0, changes == 0, d.getUnusedIndexes(tableName))
			d.issues = append(d.issues, utils.Issue{IssueType: "TableSizeLarge", Target: tableName, Severity: utils.Medium, Detail: detail, Solution: "REVIEW table - consider partitioning and/or pruning\n"})
		}
	}
}

func (d *TableIssues) getUnusedIndexes(tableName string) string {
	sub := IndexIssues{}
	sub.Init(d.context, d.datasource)
	sub.Execute(tableName)
	unused := sub.GetIssues()

	if len(unused) == 0 {
		return ""
	}

	unusedIndexes := "Unused Indexes: "
	for i, issue := range unused {
		if i != 0 {
			unusedIndexes += ", "
		}
		unusedIndexes += issue.Target
	}
	unusedIndexes += "\n"

	return unusedIndexes
}

func (d *TableIssues) GetIssues() []utils.Issue {
	return d.issues
}

func (d *TableIssues) GetDurationMS() int64 {
	return d.timing.GetDurationMS()
}
