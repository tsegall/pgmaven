package plugins

import (
	"database/sql"
	"fmt"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
	"time"
)

type Queries struct {
	datasource *dbutils.DataSource
	issues     []utils.Issue
	durationMS int64
}

func (d *Queries) Init(ds *dbutils.DataSource) {
	d.datasource = ds
}

// Queries - report queries with significant impact on the system.
func (d *Queries) Execute(args ...string) {
	startMS := time.Now().UnixMilli()
	d.issues = make([]utils.Issue, 0)

	_, min := d.datasource.ExecuteQueryRow(`select min(insert_dt)::text from pgmaven_pg_stat_statements`)
	_, max := d.datasource.ExecuteQueryRow(`select max(insert_dt)::text from pgmaven_pg_stat_statements`)

	query := `SELECT usename, calls, mean_exec_time, total_exec_time, queryid, query
	FROM pgmaven_pg_stat_statements pgss, pg_user pgu
	WHERE pgss.userid = pgu.usesysid
	AND pgss.insert_dt = $1
	AND pgu.usename NOT IN ('rdsrepladmin', 'rdsadmin', 'rdstopmgr');`

	_ = d.datasource.ExecuteQueryRows(query, []any{min}, queryProcessor, d)
	_ = d.datasource.ExecuteQueryRows(query, []any{max}, queryProcessor, d)

	d.durationMS = time.Now().UnixMilli() - startMS
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

func (d *Queries) GetIssues() []utils.Issue {
	return nil
}

func (d *Queries) GetDurationMS() int64 {
	return d.durationMS
}
