package plugins

import (
	"database/sql"
	"fmt"
	"log"

	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type SillyIndexes struct {
	issues []utils.Issue
}

const smallTable int64 = 10000

// SillyIndexes reports on indexes that seem to be silly for a variety of reasons.
func (s *SillyIndexes) Execute(args ...string) {
	s.issues = make([]utils.Issue, 0)

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

	err := dbutils.ExecuteQueryRows(tableQuery, []any{dbutils.GetSchema(), smallTable}, tableProcessor, s)
	if err != nil {
		log.Printf("ERROR: Table query failed with error: %v\n", err)
	}
}

func tableProcessor(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}, self any) {
	s := self.(*SillyIndexes)
	tableName := string((*values[0].(*interface{})).([]uint8))

	query := fmt.Sprintf(`select count(*) from %s`, tableName)
	err, rows := dbutils.ExecuteQueryRow(query)
	if err != nil {
		log.Printf("ERROR: Query '%s' failed with error: %v\n", query, err)
	}

	rowsInt := rows.(int64)
	if rowsInt < smallTable {
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
			 WHERE inh.inhrelid = stat.indexrelid);
		`
		err := dbutils.ExecuteQueryRows(sillyIndexQuery, []any{dbutils.GetSchema(), tableName}, sillyIndexProcessor, s)
		if err != nil {
			log.Printf("ERROR: SillyIndexQuery failed with error: %v\n", err)
		}

	} else {
		s.issues = append(s.issues, utils.Issue{IssueType: "AnalyzeSuggested", Detail: "n_live_tup < row count\n", Solution: fmt.Sprintf("ANALYZE \"%s\"\n", tableName)})
	}

}

func sillyIndexProcessor(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}, self any) {
	s := self.(*SillyIndexes)
	tableName := string((*values[1].(*interface{})).([]uint8))
	indexName := string((*values[2].(*interface{})).([]uint8))
	indexSize := (*values[3].(*interface{})).(int64)

	tableDetail := fmt.Sprintf("Table: %s, Index Size: %d, Silly indexes (%s)\n", tableName, indexSize, indexName)
	index1Definition := dbutils.IndexDefinition(quote(indexName))
	indexDetail := fmt.Sprintf("Index definition: '%s'\n", index1Definition)

	s.issues = append(s.issues, utils.Issue{IssueType: "SillyIndex", Detail: tableDetail + indexDetail, Solution: fmt.Sprintf("DROP INDEX \"%s\"\n", indexName)})
}

func (s *SillyIndexes) GetIssues() []utils.Issue {
	return s.issues
}
