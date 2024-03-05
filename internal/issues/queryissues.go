package issues

import (
	"bufio"
	"database/sql"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
	"sort"
	"strconv"
	"strings"
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

type QueryIssues struct {
	datasource     *dbutils.DataSource
	context        utils.Context
	issues         []utils.Issue
	startQuery     map[int64]query
	endQuery       map[int64]query
	timing         utils.Timing
	hashDecoder    map[string]string
	patternDecoder map[string]string
}

func (d *QueryIssues) Init(context utils.Context, ds *dbutils.DataSource) {
	d.datasource = ds
	d.context = context
	d.initDecoder()
}

func (d *QueryIssues) initDecoder() {
	d.hashDecoder = make(map[string]string)
	d.patternDecoder = make(map[string]string)
	file, err := os.Open("decoder.txt")
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		elts := strings.Split(line, " -> ")
		if elts[0][0] == '/' {
			d.patternDecoder[elts[0][1:len(elts[0])-1]] = elts[1]
		} else {
			d.hashDecoder[elts[0]] = elts[1]
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func (d *QueryIssues) decode(query string) string {
	h := strconv.FormatUint(uint64(hash(query)), 16)
	ret, ok := d.hashDecoder[h]
	if ok {
		return ret
	}
	for k, v := range d.patternDecoder {
		if strings.Contains(query, k) {
			return v
		}
	}

	return ""
}

// Queries - report queries with significant impact on the system.
func (d *QueryIssues) Execute(args ...string) {
	startMS := time.Now().UnixMilli()
	d.issues = make([]utils.Issue, 0)
	d.startQuery = make(map[int64]query)
	d.endQuery = make(map[int64]query)

	end := time.Now().Add(-d.context.DurationOffset)
	start := end.Add(-d.context.Duration)
	endClosest := d.datasource.GetClosest("pgmaven_pg_stat_statements", end).(time.Time)
	startClosest := d.datasource.GetClosest("pgmaven_pg_stat_statements", start).(time.Time)

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
	AND total_exec_time != 0       -- Ditch Explains and Prepares
	AND pgss.insert_dt = $1
	AND total_exec_time > $2
	AND pgu.usename NOT IN ('rdsrepladmin', 'rdsadmin', 'rdstopmgr');`

	startQuery := `
SELECT usename, calls, mean_exec_time, total_exec_time, queryid, query
	FROM pgmaven_pg_stat_statements pgss, pg_user pgu
	WHERE pgss.userid = pgu.usesysid
	AND total_exec_time != 0
    AND insert_dt = $1
	AND queryid in (
		SELECT queryid
		FROM pgmaven_pg_stat_statements pgss, pg_user pgu
		WHERE pgss.userid = pgu.usesysid
		AND total_exec_time != 0
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

	if d.context.Verbose {
		fmt.Printf("Analysis period: %v - %v (%v)\n", startClosest, endClosest, endClosest.Sub(startClosest))
	}

	total_exec_time := 0.0
	for k, v := range d.endQuery {
		startElement, ok := d.startQuery[k]
		if ok {
			v.calls -= startElement.calls
			v.total_exec_time -= startElement.total_exec_time
			d.endQuery[k] = v
		}
		total_exec_time += v.total_exec_time
	}

	keys := maps.Keys(d.endQuery)
	sort.SliceStable(keys, func(i, j int) bool {
		return d.endQuery[keys[i]].total_exec_time > d.endQuery[keys[j]].total_exec_time
	})

	fmt.Println("username,calls,mean_exec_time,duration,percent,queryid,hash,source,query")
	for _, k := range keys {
		v := d.endQuery[k]
		dur := time.Duration(v.total_exec_time * float64(time.Millisecond))
		h := strconv.FormatUint(uint64(hash(v.queryText)), 16)
		fmt.Printf("%s,%d,%.2f,%v,%.2f,%d,%s,%s,%s\n",
			v.userName, v.calls, v.mean_exec_time, dur, (v.total_exec_time*100)/total_exec_time, v.queryId, h,
			d.decode(v.queryText), utils.QuoteAlways(utils.RemoveBlankLines(v.queryText)))
	}
	d.timing.SetDurationMS(time.Now().UnixMilli() - startMS)
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

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

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

func (d *QueryIssues) GetIssues() []utils.Issue {
	return nil
}

func (d *QueryIssues) GetDurationMS() int64 {
	return d.timing.GetDurationMS()
}
