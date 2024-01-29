package plugins

import (
	"database/sql"
	"fmt"
	"log"

	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type UnusedIndexes struct {
	issues []utils.Issue
}

// UnusedIndexes reports on unused indexes.
func (u *UnusedIndexes) Execute(args ...string) {
	u.issues = make([]utils.Issue, 0)

	unusedIndexQuery := `
	SELECT
	stat.schemaname,
	stat.relname AS tablename,
	stat.indexrelname AS indexname,
	pg_relation_size(stat.indexrelid) AS index_size
  FROM pg_catalog.pg_stat_user_indexes stat
  JOIN pg_catalog.pg_index i using (indexrelid)
  JOIN pg_catalog.pg_indexes i2 ON stat.schemaname = i2.schemaname AND stat.relname = i2.tablename AND stat.indexrelname = i2.indexname
  WHERE stat.idx_scan = 0                -- has never been scanned
  and i2.indexdef like '%USING btree%'   -- only want BTREE indexes
  AND 0 <>ALL (i.indkey)                 -- no index column is an expression
  AND NOT i.indisunique                  -- is not a UNIQUE index
  AND NOT EXISTS                         -- does not enforce a constraint
	(SELECT 1 FROM pg_catalog.pg_constraint c
	 WHERE c.conindid = stat.indexrelid)
  AND NOT EXISTS                         -- is not an index partition
	(SELECT 1 FROM pg_catalog.pg_inherits AS inh
	 WHERE inh.inhrelid = stat.indexrelid);
`
	err := dbutils.ExecuteQueryRows(unusedIndexQuery, nil, unusedIndexProcessor, u)
	if err != nil {
		log.Printf("ERROR: UnusedIndexQuery failed with error: %v\n", err)
	}
}

func quote(s string) string {
	return "\"" + s + "\""
}

// unusedIndexProcessor is invoked for every row of the Unused Index Query.
// The Query returns a row with the following format (schemaname, tablename, indexname, index_size)
func unusedIndexProcessor(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}, self any) {
	u := self.(*UnusedIndexes)
	tableName := string((*values[1].(*interface{})).([]uint8))
	indexName := string((*values[2].(*interface{})).([]uint8))
	indexSize := (*values[3].(*interface{})).(int64)

	tableDetail := fmt.Sprintf("Table: %s, Index Size: %d, Unused indexes (%s)\n", tableName, indexSize, indexName)
	index1Definition := dbutils.IndexDefinition(quote(indexName))
	indexDetail := fmt.Sprintf("Index definition: '%s'\n", index1Definition)

	u.issues = append(u.issues, utils.Issue{IssueType: "UnusedIndex", Detail: tableDetail + indexDetail, Solution: fmt.Sprintf("DROP INDEX \"%s\"\n", indexName)})
}

func (u *UnusedIndexes) GetIssues() []utils.Issue {
	return u.issues
}
