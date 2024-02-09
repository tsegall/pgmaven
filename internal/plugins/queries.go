package plugins

import (
	"database/sql"
	"fmt"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
	"sort"
	"time"

	"golang.org/x/exp/maps"
)

type query struct {
	userName        string
	calls           int64
	mean_exec_time  float64
	total_exec_time float64
	queryId         int64
	queryText       string
}

type Queries struct {
	datasource *dbutils.DataSource
	context    utils.Context
	issues     []utils.Issue
	durationMS int64
	startQuery map[int64]query
	endQuery   map[int64]query
}

func (d *Queries) Init(context utils.Context, ds *dbutils.DataSource) {
	d.datasource = ds
	d.context = context
}

func (d *Queries) getClosest(t time.Time) any {
	query := `with
	date_options as (
	select
		distinct(insert_dt) as insert_dt
	from
		pgmaven_pg_stat_statements),
	closest as (
	select
		insert_dt,
		abs(extract(epoch from insert_dt - $1)) as diff
	from
		date_options
	order by
		diff asc
	limit 1)
	select
		insert_dt
	from
		closest
	`
	closest, _ := d.datasource.ExecuteQueryRow(query, []any{t})

	return closest
}

// Queries - report queries with significant impact on the system.
func (d *Queries) Execute(args ...string) {
	startMS := time.Now().UnixMilli()
	d.issues = make([]utils.Issue, 0)
	d.startQuery = make(map[int64]query)
	d.endQuery = make(map[int64]query)

	end := time.Now().Add(-d.context.DurationOffset)
	start := end.Add(-d.context.Duration)
	endClosest := d.getClosest(end).(time.Time)
	startClosest := d.getClosest(start).(time.Time)

	totalExecTime, _ := d.datasource.ExecuteQueryRow(`select sum(total_exec_time) from pgmaven_pg_stat_statements where insert_dt = $1`, []any{endClosest})
	totalExecTimeMS := totalExecTime.(float64)

	if d.context.Verbose {
		fmt.Printf("Analyzing load for duration: %v (%v - %v) - Total Exec Time: %f\n", d.context.Duration, start, end, totalExecTimeMS)
	}

	// Find all queries responsible for at least 1% of the CPU
	timeCutoffMS := totalExecTimeMS / 100

	endQuery := `
SELECT usename, calls, mean_exec_time, total_exec_time, queryid, query
	FROM pgmaven_pg_stat_statements pgss, pg_user pgu
	WHERE pgss.userid = pgu.usesysid
	AND pgss.insert_dt = $1
	AND total_exec_time > $2
	AND pgu.usename NOT IN ('rdsrepladmin', 'rdsadmin', 'rdstopmgr');`

	startQuery := `
SELECT usename, calls, mean_exec_time, total_exec_time, queryid, query
	FROM pgmaven_pg_stat_statements pgss, pg_user pgu
	WHERE pgss.userid = pgu.usesysid
    AND insert_dt = $1
	AND queryid in (
		SELECT queryid
		FROM pgmaven_pg_stat_statements pgss, pg_user pgu
		WHERE pgss.userid = pgu.usesysid
		AND pgss.insert_dt = $2
		AND total_exec_time > $3
		AND pgu.usename NOT IN ('rdsrepladmin', 'rdsadmin', 'rdstopmgr'));`

	_ = d.datasource.ExecuteQueryRows(endQuery, []any{endClosest, timeCutoffMS}, queryProcessor, d.endQuery)
	_ = d.datasource.ExecuteQueryRows(startQuery, []any{startClosest, endClosest, timeCutoffMS}, queryProcessor, d.startQuery)

	if len(d.startQuery) != len(d.endQuery) {
		fmt.Printf("WARNING: not all queries matched in period requested, data suspect, end: %d, start: %d\n", len(d.endQuery), len(d.startQuery))
		for _, queryId := range difference(maps.Keys(d.endQuery), maps.Keys(d.startQuery)) {
			fmt.Printf("\tQueryId: %d\n", queryId)
		}
	}

	fmt.Printf("Analysis period: %v - %v (%v)\n", startClosest, endClosest, endClosest.Sub(startClosest))

	total_exec_time := 0.0
	for k, v := range d.endQuery {
		startElement, ok := d.startQuery[k]
		if ok {
			v.calls -= startElement.calls
			v.total_exec_time -= startElement.total_exec_time
		}
		total_exec_time += v.total_exec_time
	}

	keys := maps.Keys(d.endQuery)
	sort.SliceStable(keys, func(i, j int) bool {
		return d.endQuery[keys[i]].total_exec_time > d.endQuery[keys[j]].total_exec_time
	})

	for _, k := range keys {
		v := d.endQuery[k]
		fmt.Printf("%s, %d, %.2f, %.2f, %.2f, %d, %s\n", v.userName, v.calls, v.mean_exec_time, v.total_exec_time, (v.total_exec_time*100)/totalExecTimeMS, v.queryId, v.queryText)
	}
	d.durationMS = time.Now().UnixMilli() - startMS
}

// difference returns the elements in `a` that aren't in `b`.
func difference(a, b []int64) []int64 {
	mb := make(map[int64]struct{}, len(b))
	for _, x := range b {
		mb[x] = struct{}{}
	}
	var diff []int64
	for _, x := range a {
		if _, found := mb[x]; !found {
			diff = append(diff, x)
		}
	}
	return diff
}

// AnalyzeTableProcessor is invoked for every row of the Analyze Table Query.
// The Query returns a row with the following format (n_live_tup, insert_dt)
func queryProcessor(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}, self any) {
	m := self.(map[int64]query)
	userName := string((*values[0].(*interface{})).([]uint8))
	calls := (*values[1].(*interface{})).(int64)
	mean_exec_time := (*values[2].(*interface{})).(float64)
	total_exec_time := (*values[3].(*interface{})).(float64)
	queryId := (*values[4].(*interface{})).(int64)
	queryText := (*values[5].(*interface{})).(string)
	m[queryId] = query{userName, calls, mean_exec_time, total_exec_time, queryId, queryText}
}

func (d *Queries) GetIssues() []utils.Issue {
	return nil
}

func (d *Queries) GetDurationMS() int64 {
	return d.durationMS
}
