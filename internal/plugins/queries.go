package plugins

import (
	"database/sql"
	"fmt"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type Queries struct {
	issues []utils.Issue
}

// Queries - report queries with significant impact on the system.
func (q *Queries) Execute(args ...string) {
	q.issues = make([]utils.Issue, 0)

	_, min := dbutils.ExecuteQueryRow(`select min(insert_dt)::text from pgmaven_pg_stat_statements`)
	_, max := dbutils.ExecuteQueryRow(`select max(insert_dt)::text from pgmaven_pg_stat_statements`)

	query := `SELECT usename, calls, mean_exec_time, total_exec_time, queryid, query
	FROM pgmaven_pg_stat_statements pgss, pg_user pgu
	WHERE pgss.userid = pgu.usesysid
	AND pgss.insert_dt = $1
	AND pgu.usename NOT IN ('rdsrepladmin', 'rdsadmin', 'rdstopmgr');`

	_ = dbutils.ExecuteQueryRows(query, []any{min}, queryProcessor, q)
	_ = dbutils.ExecuteQueryRows(query, []any{max}, queryProcessor, q)
}

// AnalyzeTableProcessor is invoked for every row of the Analyze Table Query.
// The Query returns a row with the following format (n_live_tup, insert_dt)
func queryProcessor(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}, self any) {
	userName := string((*values[0].(*interface{})).([]uint8))
	calls := (*values[1].(*interface{})).(int64)
	mean_exec_time := (*values[2].(*interface{})).(float64)
	total_exec_time := (*values[3].(*interface{})).(float64)
	queryId := (*values[4].(*interface{})).(int64)
	query := (*values[5].(*interface{})).(string)
	fmt.Printf("%s, %d, %.2f, %.2f, %d, %s\n", userName, calls, mean_exec_time, total_exec_time, queryId, query)
}

func (q *Queries) GetIssues() []utils.Issue {
	return nil
}
