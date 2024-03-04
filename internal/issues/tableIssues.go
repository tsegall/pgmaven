package issues

import (
	"database/sql"
	"fmt"
	"log"
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

	if d.isIssueEnabled("TableBloat") {
		d.doTableBloat()

		if d.specificIssueEnabled() {
			d.timing.SetDurationMS(time.Now().UnixMilli() - startMS)

			return
		}
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

func (d *TableIssues) specificIssueEnabled() bool {
	return d.specificIssue != ""
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

func (d *TableIssues) doTableBloat() {

	tableBloatQuery := `
WITH constants AS (
    -- define some constants for sizes of things
    -- for reference down the query and easy maintenance
    SELECT current_setting('block_size')::numeric AS bs, 23 AS hdr, 8 AS ma
),
no_stats AS (
    -- screen out table who have attributes
    -- which dont have stats, such as JSON
    SELECT table_schema, table_name,
        n_live_tup::numeric as est_rows,
        pg_table_size(relid)::numeric as table_size
    FROM information_schema.columns
        JOIN pg_stat_user_tables as psut
           ON table_schema = psut.schemaname
           AND table_name = psut.relname
        LEFT OUTER JOIN pg_stats
        ON table_schema = pg_stats.schemaname
            AND table_name = pg_stats.tablename
            AND column_name = attname
    WHERE attname IS NULL
        AND table_schema NOT IN ('pg_catalog', 'information_schema')
    GROUP BY table_schema, table_name, relid, n_live_tup
),
null_headers AS (
    -- calculate null header sizes
    -- omitting tables which dont have complete stats
    -- and attributes which aren't visible
    SELECT
        hdr+1+(sum(case when null_frac <> 0 THEN 1 else 0 END)/8) as nullhdr,
        SUM((1-null_frac)*avg_width) as datawidth,
        MAX(null_frac) as maxfracsum,
        schemaname,
        tablename,
        hdr, ma, bs
    FROM pg_stats CROSS JOIN constants
        LEFT OUTER JOIN no_stats
            ON schemaname = no_stats.table_schema
            AND tablename = no_stats.table_name
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        AND no_stats.table_name IS NULL
        AND EXISTS ( SELECT 1
            FROM information_schema.columns
                WHERE schemaname = columns.table_schema
                    AND tablename = columns.table_name )
    GROUP BY schemaname, tablename, hdr, ma, bs
),
data_headers AS (
    -- estimate header and row size
    SELECT
        ma, bs, hdr, schemaname, tablename,
        (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
        (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
    FROM null_headers
),
table_estimates AS (
    -- make estimates of how large the table should be
    -- based on row and page size
    SELECT schemaname, tablename, bs,
        reltuples::numeric as est_rows, relpages * bs as table_bytes,
    CEIL((reltuples*
            (datahdr + nullhdr2 + 4 + ma -
                (CASE WHEN datahdr%ma=0
                    THEN ma ELSE datahdr%ma END)
                )/(bs-20))) * bs AS expected_bytes,
        reltoastrelid
    FROM data_headers
        JOIN pg_class ON tablename = relname
        JOIN pg_namespace ON relnamespace = pg_namespace.oid
            AND schemaname = nspname
    WHERE pg_class.relkind = 'r'
),
estimates_with_toast AS (
    -- add in estimated TOAST table sizes
    -- estimate based on 4 toast tuples per page because we dont have
    -- anything better.  also append the no_data tables
    SELECT schemaname, tablename,
        TRUE as can_estimate,
        est_rows,
        table_bytes + ( coalesce(toast.relpages, 0) * bs ) as table_bytes,
        expected_bytes + ( ceil( coalesce(toast.reltuples, 0) / 4 ) * bs ) as expected_bytes
    FROM table_estimates LEFT OUTER JOIN pg_class as toast
        ON table_estimates.reltoastrelid = toast.oid
            AND toast.relkind = 't'
),
table_estimates_plus AS (
-- add some extra metadata to the table data
-- and calculations to be reused
-- including whether we cant estimate it
-- or whether we think it might be compressed
    SELECT current_database() as databasename,
            schemaname, tablename, can_estimate,
            est_rows,
            CASE WHEN table_bytes > 0
                THEN table_bytes::NUMERIC
                ELSE NULL::NUMERIC END
                AS table_bytes,
            CASE WHEN expected_bytes > 0
                THEN expected_bytes::NUMERIC
                ELSE NULL::NUMERIC END
                    AS expected_bytes,
            CASE WHEN expected_bytes > 0 AND table_bytes > 0
                AND expected_bytes <= table_bytes
                THEN (table_bytes - expected_bytes)::NUMERIC
                ELSE 0::NUMERIC END AS bloat_bytes
    FROM estimates_with_toast
    UNION ALL
    SELECT current_database() as databasename,
        table_schema, table_name, FALSE,
        est_rows, table_size,
        NULL::NUMERIC, NULL::NUMERIC
    FROM no_stats
),
bloat_data AS (
    -- do final math calculations and formatting
    select current_database() as databasename,
        schemaname, tablename, can_estimate,
        table_bytes, round(table_bytes/(1024^2)::NUMERIC,3) as table_mb,
        expected_bytes, round(expected_bytes/(1024^2)::NUMERIC,3) as expected_mb,
        round(bloat_bytes*100/table_bytes) as pct_bloat,
        round(bloat_bytes/(1024::NUMERIC^2),2) as mb_bloat,
        table_bytes, expected_bytes, est_rows
    FROM table_estimates_plus
)
-- filter output for bloated tables
SELECT schemaname, tablename,
    can_estimate,
    est_rows,
    pct_bloat, mb_bloat,
    table_mb
FROM bloat_data
-- this where clause defines which tables actually appear
-- in the bloat chart
-- example below filters for tables which are either 50%
-- bloated and more than 20mb in size, or more than 25%
-- bloated and more than 1GB in size
WHERE ( pct_bloat >= 50 AND mb_bloat >= 20 )
    OR ( pct_bloat >= 25 AND mb_bloat >= 1000 )
ORDER BY pct_bloat DESC;`

	err := d.datasource.ExecuteQueryRows(tableBloatQuery, nil, tableBloatProcessor, d)
	if err != nil {
		log.Printf("ERROR: Database: %s, Overlapping index query failed with error: %v\n", d.datasource.GetDBName(), err)
	}
}

func tableBloatProcessor(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}, self any) {
	d := self.(*TableIssues)
	tableName := string((*values[1].(*interface{})).([]uint8))
	estRows := string((*values[3].(*interface{})).([]uint8))
	pctBloat := string((*values[4].(*interface{})).([]uint8))

	detail := fmt.Sprintf("Table: %s, Bloat: %s%%, Estimated Rows: %s\n", tableName, pctBloat, estRows)

	d.issues = append(d.issues, utils.Issue{IssueType: "TableBloat", Target: tableName, Detail: detail, Severity: utils.Medium,
		Solution: fmt.Sprintf("VACUUM \"%s\"\n", tableName)})
}

func (d *TableIssues) GetIssues() []utils.Issue {
	return d.issues
}

func (d *TableIssues) GetDurationMS() int64 {
	return d.timing.GetDurationMS()
}
