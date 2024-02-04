package plugins

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type DuplicateIndexes struct {
	datasource *dbutils.DataSource
	issues     []utils.Issue
	durationMS int64
}

func (d *DuplicateIndexes) Init(context utils.Context, ds *dbutils.DataSource) {
	d.datasource = ds
}

// DuplicateIndexes reports on redundant indexes.
func (d *DuplicateIndexes) Execute(args ...string) {
	startMS := time.Now().UnixMilli()
	d.issues = make([]utils.Issue, 0)

	duplicateIndexQuery := `
SELECT table_name, pg_size_pretty(sum(pg_relation_size(idx))::bigint) as size,
	(array_agg(idx))[1] as idx1, (array_agg(idx))[2] as idx2,
	(array_agg(idx))[3] as idx3, (array_agg(idx))[4] as idx4
FROM (
 SELECT indexrelid::regclass as idx, indrelid::regclass as table_name, (indrelid::text ||E'\n'|| indclass::text ||E'\n'|| indkey::text ||E'\n'||
									  coalesce(indexprs::text,'')||E'\n' || coalesce(indpred::text,'')) as key
 FROM pg_index) sub
GROUP BY table_name, key HAVING count(*)>1
ORDER BY sum(pg_relation_size(idx)) DESC;
`
	err := d.datasource.ExecuteQueryRows(duplicateIndexQuery, nil, duplicateIndexProcessor, d)
	if err != nil {
		log.Printf("ERROR: DuplicateIndexQuery failed with error: %v\n", err)
	}

	d.durationMS = time.Now().UnixMilli() - startMS
}

// duplicateIndexProcess is invoked for every row of the Duplicate Index Query.
// The Query returns a row with the following format (tableName, index size, index1, index2) - where index1 and index2 are duplicated.
func duplicateIndexProcessor(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}, self any) {
	d := self.(*DuplicateIndexes)
	tableName := string((*values[0].(*interface{})).([]uint8))
	indexSize := (*values[1].(*interface{})).(string)
	index1 := string((*values[2].(*interface{})).([]uint8))
	index2 := string((*values[3].(*interface{})).([]uint8))

	tableDetail := fmt.Sprintf("Table: %s, Index Size: %s, Duplicate indexes (%s, %s)\n", tableName, indexSize, index1, index2)
	index1Definition := d.datasource.IndexDefinition(index1)
	index2Definition := d.datasource.IndexDefinition(index2)
	indexDetail := fmt.Sprintf("First Index: '%s'\nSecond Index: '%s'\n", index1Definition, index2Definition)

	// If Index 2 is unique then kill Index 1
	if strings.Contains(index2Definition, " UNIQUE ") {
		d.issues = append(d.issues, utils.Issue{IssueType: "DuplicateIndex", Target: index1, Detail: tableDetail + indexDetail, Solution: fmt.Sprintf("DROP INDEX %s\n", index1)})
		return
	}

	d.issues = append(d.issues, utils.Issue{IssueType: "DuplicateIndex", Target: index2, Detail: tableDetail + indexDetail, Solution: fmt.Sprintf("DROP INDEX %s\n", index2)})
}

func (d *DuplicateIndexes) GetIssues() []utils.Issue {
	return d.issues
}

func (d *DuplicateIndexes) GetDurationMS() int64 {
	return d.durationMS
}
