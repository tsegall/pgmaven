package commands

import (
	"database/sql"
	"fmt"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
	"time"
)

type NewActivity struct {
	datasource *dbutils.DataSource
	context    utils.Context
}

func (c *NewActivity) Init(context utils.Context, ds *dbutils.DataSource) {
	c.datasource = ds
	c.context = context
}

func (c *NewActivity) Execute(args ...string) {
	end := time.Now().Add(-c.context.DurationOffset)
	start := end.Add(-c.context.Duration)
	endClosest := c.datasource.GetClosest("pgmaven_pg_stat_statements", end).(time.Time)
	startClosest := c.datasource.GetClosest("pgmaven_pg_stat_statements", start).(time.Time)

	if c.context.Verbose {
		fmt.Printf("Analyze new queries from %v to %v\n", startClosest, endClosest)
	}

	newStatementQuery := `
select pgu.usename, calls, mean_exec_time, total_exec_time, queryid, query, min(insert_dt)
from
	pgmaven_pg_stat_statements pgss, pg_user pgu
where
	pgss.userid = pgu.usesysid
	and total_exec_time != 0     -- Ditch Explains and Prepares
	and pgu.usename not in ('rdsrepladmin', 'rdsadmin', 'rdstopmgr')
	and query not ilike '%pgmaven%'
	and queryid in 
(
	select
		distinct(queryid)
	from
		pgmaven_pg_stat_statements
	where
		insert_dt = $1
except
	select
		distinct(queryid)
	from
		pgmaven_pg_stat_statements
	where
		insert_dt <= $2)
group by pgu.usename, calls, mean_exec_time, total_exec_time, queryid, query
order by min(insert_dt)`

	_ = c.datasource.ExecuteQueryRows(newStatementQuery, []any{endClosest, startClosest}, newQueryProcessor, c)

	newIndexQuery := `select 
schemaname,
relname AS tablename,
indexrelname AS indexname,
pg_relation_size(indexrelid) AS index_size,
pg_table_size(indexrelid) as table_size
FROM pg_stat_user_indexes
where 	indexrelname in 
(
SELECT
ppsui.indexrelname
FROM pgmaven_pg_stat_user_indexes ppsui
JOIN pg_catalog.pg_index i using (indexrelid)
JOIN pg_catalog.pg_indexes i2 ON ppsui.schemaname = i2.schemaname AND ppsui.relname = i2.tablename AND ppsui.indexrelname = i2.indexname
WHERE ppsui.idx_scan != 0                -- has never been scanned
and ppsui.schemaname = $1
AND i2.indexdef like '%%USING btree%%'   -- only want BTREE indexes
and insert_dt = $2
AND 0 <>ALL (i.indkey)                 -- no index column is an expression
AND NOT i.indisunique                  -- is not a UNIQUE index
AND NOT EXISTS                         -- does not enforce a constraint
(SELECT 1 FROM pg_catalog.pg_constraint c
	WHERE c.conindid = ppsui.indexrelid)
AND NOT EXISTS                         -- is not an index partition
(SELECT 1 FROM pg_catalog.pg_inherits AS inh
	WHERE inh.inhrelid = ppsui.indexrelid)
except 
SELECT
ppsui.indexrelname
FROM pgmaven_pg_stat_user_indexes ppsui
JOIN pg_catalog.pg_index i using (indexrelid)
JOIN pg_catalog.pg_indexes i2 ON ppsui.schemaname = i2.schemaname AND ppsui.relname = i2.tablename AND ppsui.indexrelname = i2.indexname
WHERE ppsui.idx_scan != 0                -- has never been scanned
and ppsui.schemaname = $3
AND i2.indexdef like '%%USING btree%%'   -- only want BTREE indexes
and insert_dt = $4
AND 0 <>ALL (i.indkey)                 -- no index column is an expression
AND NOT i.indisunique                  -- is not a UNIQUE index
AND NOT EXISTS                         -- does not enforce a constraint
(SELECT 1 FROM pg_catalog.pg_constraint c
	WHERE c.conindid = ppsui.indexrelid)
AND NOT EXISTS                         -- is not an index partition
(SELECT 1 FROM pg_catalog.pg_inherits AS inh
	WHERE inh.inhrelid = ppsui.indexrelid)
	)
`

	endClosest = c.datasource.GetClosest("pgmaven_pg_stat_user_indexes", end).(time.Time)
	startClosest = c.datasource.GetClosest("pgmaven_pg_stat_user_indexes", start).(time.Time)

	if c.context.Verbose {
		fmt.Printf("Analyze new index use from %v to %v\n", startClosest, endClosest)
	}

	_ = c.datasource.ExecuteQueryRows(newIndexQuery, []any{c.datasource.GetSchema(), endClosest, c.datasource.GetSchema(), startClosest}, newIndexProcessor, c)

}

func newQueryProcessor(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}, self any) {
	userName := string((*values[0].(*interface{})).([]uint8))
	calls := (*values[1].(*interface{})).(int64)
	mean_exec_time := (*values[2].(*interface{})).(float64)
	total_exec_time := (*values[3].(*interface{})).(float64)
	queryId := (*values[4].(*interface{})).(int64)
	queryText := (*values[5].(*interface{})).(string)
	minInsertDt := (*values[6].(*interface{})).(time.Time)

	if rowNumber == 1 {
		fmt.Println("username,calls,mean_exec_time,total_exec_time,queryid,insert_dt,query")
	}
	fmt.Printf("%s,%d,%.2f,%.2f,%d,%v,%s\n", userName, calls, mean_exec_time, total_exec_time, queryId, minInsertDt, utils.QuoteAlways(queryText))
}

func newIndexProcessor(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}, self any) {
	schemaName := string((*values[0].(*interface{})).([]uint8))
	tableName := string((*values[1].(*interface{})).([]uint8))
	indexName := string((*values[2].(*interface{})).([]uint8))
	indexSize := (*values[3].(*interface{})).(int64)
	tableSize := (*values[4].(*interface{})).(int64)

	if rowNumber == 1 {
		fmt.Println("schema,table,index,indexSize,tableSize")
	}
	fmt.Printf("%s,%s,%s,%d,%d\n", schemaName, tableName, indexName, indexSize, tableSize)
}
