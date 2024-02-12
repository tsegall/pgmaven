package plugins

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type UnusedIndexes struct {
	datasource *dbutils.DataSource
	issues     []utils.Issue
	durationMS int64
}

func (d *UnusedIndexes) Init(context utils.Context, ds *dbutils.DataSource) {
	d.datasource = ds
}

// UnusedIndexes reports on unused indexes.
func (d *UnusedIndexes) Execute(args ...string) {
	startMS := time.Now().UnixMilli()
	d.issues = make([]utils.Issue, 0)

	tableClause := ""
	if len(args) != 0 {
		tableClause = "AND stat.relname = '" + args[0] + "'"
	}

	unusedIndexQuery := fmt.Sprintf(`
WITH stat as (
	SELECT schemaname, relname, indexrelname, indexrelid, max(idx_scan) as scans
		FROM pgmaven_pg_stat_user_indexes ppsui
		GROUP BY schemaname, relname, indexrelname, indexrelid)
	SELECT
		stat.schemaname,
		stat.relname AS tablename,
		stat.indexrelname AS indexname,
		pg_relation_size(stat.indexrelid) AS index_size,
		pg_table_size(stat.indexrelid) as table_size
		FROM stat
		JOIN pg_catalog.pg_index i using (indexrelid)
		JOIN pg_catalog.pg_indexes i2 ON stat.schemaname = i2.schemaname AND stat.relname = i2.tablename AND stat.indexrelname = i2.indexname
		WHERE stat.scans = 0                -- has never been scanned
		and i2.indexdef like '%%USING btree%%'   -- only want BTREE indexes
		AND 0 <>ALL (i.indkey)                 -- no index column is an expression
		AND NOT i.indisunique                  -- is not a UNIQUE index
		%s
		AND NOT EXISTS                         -- does not enforce a constraint
		(SELECT 1 FROM pg_catalog.pg_constraint c
			WHERE c.conindid = stat.indexrelid)
		AND NOT EXISTS                         -- is not an index partition
		(SELECT 1 FROM pg_catalog.pg_inherits AS inh
			WHERE inh.inhrelid = stat.indexrelid)
		ORDER by tablename asc, indexname asc;
`, tableClause)
	err := d.datasource.ExecuteQueryRows(unusedIndexQuery, nil, unusedIndexProcessor, d)
	if err != nil {
		log.Printf("ERROR: UnusedIndexQuery failed with error: %v\n", err)
	}

	d.durationMS = time.Now().UnixMilli() - startMS
}

func quote(s string) string {
	return "\"" + s + "\""
}

// unusedIndexProcessor is invoked for every row of the Unused Index Query.
// The Query returns a row with the following format (schemaname, tablename, indexname, index_size)
func unusedIndexProcessor(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}, self any) {
	d := self.(*UnusedIndexes)
	tableName := string((*values[1].(*interface{})).([]uint8))
	indexName := string((*values[2].(*interface{})).([]uint8))
	indexSize := (*values[3].(*interface{})).(int64)
	tableSize := (*values[4].(*interface{})).(int64)

	tableDetail := fmt.Sprintf("Table: %s, Index Size: %d, Table Size: %d, Unused indexes (%s)\n", tableName, indexSize, tableSize, indexName)
	index1Definition := d.datasource.IndexDefinition(quote(indexName))
	indexDetail := fmt.Sprintf("Index definition: '%s'\n", index1Definition)

	d.issues = append(d.issues, utils.Issue{IssueType: "UnusedIndex", Target: indexName, Detail: tableDetail + indexDetail, Solution: fmt.Sprintf("DROP INDEX \"%s\"\n", indexName)})
}

func (d *UnusedIndexes) GetIssues() []utils.Issue {
	return d.issues
}

func (d *UnusedIndexes) GetDurationMS() int64 {
	return d.durationMS
}
