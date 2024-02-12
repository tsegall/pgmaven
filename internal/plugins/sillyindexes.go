package plugins

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type SillyIndexes struct {
	datasource *dbutils.DataSource
	rows       int64
	issues     []utils.Issue
	durationMS int64
}

const smallTable int64 = 100

func (d *SillyIndexes) Init(context utils.Context, ds *dbutils.DataSource) {
	d.datasource = ds
}

// SillyIndexes reports on indexes that seem to be silly for a variety of reasons.
func (d *SillyIndexes) Execute(args ...string) {
	start := time.Now().UnixMilli()
	d.issues = make([]utils.Issue, 0)

	tableQuery := `
	select
	sub.table_name
from
	(
	select
		table_name
	from
		information_schema.tables
	where
		table_schema = $1
		and table_type = 'BASE TABLE'
		and table_name not ilike 'PGMAVEN_%'
except
	select
		table_name
	from
		information_schema.tables,
		pg_stat_user_tables psut
	where
		table_name = relname
		and table_schema = $1
		and table_type = 'BASE TABLE'
		and table_name not ilike 'PGMAVEN_%'
		and psut.last_analyze is not null
		and n_live_tup > $2
) as sub
order by
	table_name
`

	err := d.datasource.ExecuteQueryRows(tableQuery, []any{d.datasource.GetSchema(), smallTable}, tableProcessor, d)
	if err != nil {
		log.Printf("ERROR: Table query failed with error: %v\n", err)
	}

	d.durationMS = time.Now().UnixMilli() - start
}

func tableProcessor(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}, self any) {
	d := self.(*SillyIndexes)
	tableName := string((*values[0].(*interface{})).([]uint8))

	query := fmt.Sprintf(`select count(*) from %s`, tableName)
	rows, err := d.datasource.ExecuteQueryRow(query, nil)
	if err != nil {
		log.Printf("ERROR: Query '%s' failed with error: %v\n", query, err)
	}

	d.rows = rows.(int64)
	if d.rows < smallTable {
		sillyIndexQuery := `
		SELECT
			stat.schemaname,
			stat.relname AS tablename,
			stat.indexrelname AS indexname,
			pg_relation_size(stat.indexrelid) AS index_size
		  FROM pg_catalog.pg_stat_user_indexes stat
		  JOIN pg_catalog.pg_index i using (indexrelid)
		  JOIN pg_catalog.pg_indexes i2 ON stat.schemaname = i2.schemaname AND stat.relname = i2.tablename AND stat.indexrelname = i2.indexname
		  WHERE stat.schemaname = $1 and stat.relname = $2
		  AND stat.idx_scan != 0                 -- has been used (unused will be be picked up separately)
		  AND i2.indexdef like '%USING btree%'   -- only want BTREE indexes
		  AND 0 <>ALL (i.indkey)                 -- no index column is an expression
		  AND NOT i.indisunique                  -- is not a UNIQUE index
		  AND NOT EXISTS                         -- does not enforce a constraint
			(SELECT 1 FROM pg_catalog.pg_constraint c
			 WHERE c.conindid = stat.indexrelid)
		  AND NOT EXISTS                         -- is not an index partition
			(SELECT 1 FROM pg_catalog.pg_inherits AS inh
			 WHERE inh.inhrelid = stat.indexrelid)
		  ORDER by tablename asc, indexname asc;
		`
		err := d.datasource.ExecuteQueryRows(sillyIndexQuery, []any{d.datasource.GetSchema(), tableName}, sillyIndexProcessor, d)
		if err != nil {
			log.Printf("ERROR: SillyIndexQuery failed with error: %v\n", err)
		}

	} else {
		d.issues = append(d.issues, utils.Issue{IssueType: "AnalyzeSuggested", Target: tableName, Detail: "n_live_tup < row count\n", Solution: fmt.Sprintf("ANALYZE \"%s\"\n", tableName)})
	}
}

func sillyIndexProcessor(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}, self any) {
	d := self.(*SillyIndexes)
	tableName := string((*values[1].(*interface{})).([]uint8))
	indexName := string((*values[2].(*interface{})).([]uint8))
	indexSize := (*values[3].(*interface{})).(int64)

	tableDetail := fmt.Sprintf("Table: %s, Rows: %d, Index Size: %d, Silly indexes (%s)\n", tableName, d.rows, indexSize, indexName)
	index1Definition := d.datasource.IndexDefinition(quote(indexName))
	indexDetail := fmt.Sprintf("Index definition: '%s'\n", index1Definition)

	d.issues = append(d.issues, utils.Issue{IssueType: "SillyIndex", Target: indexName, Detail: tableDetail + indexDetail, Solution: fmt.Sprintf("DROP INDEX \"%s\"\n", indexName)})
}

func (d *SillyIndexes) GetIssues() []utils.Issue {
	return d.issues
}

func (d *SillyIndexes) GetDurationMS() int64 {
	return d.durationMS
}
