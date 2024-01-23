package plugins

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type DuplicateIndexes struct {
}

// DuplicateIndexes reports on redundant indexes.
func (command *DuplicateIndexes) Execute(args ...interface{}) {
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
	err := dbutils.ExecuteQueryRows(duplicateIndexQuery, duplicateIndexProcessor)
	if err != nil {
		log.Printf("ERROR: DuplicateIndexQuery failed with error: %v\n", err)
	}
}

// duplicateIndexProcess is invoked for every row of the Duplicate Index Query.
// The Query returns a row with the following format (tableName, index size, index1, index2) - where index1 and index2 are duplicated.
func duplicateIndexProcessor(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}) {
	tableName := string((*values[0].(*interface{})).([]uint8))
	indexSize := (*values[1].(*interface{})).(string)
	index1 := string((*values[2].(*interface{})).([]uint8))
	index2 := string((*values[3].(*interface{})).([]uint8))

	tableDetail := fmt.Sprintf("\tTable: %s, Index Size: %s, Duplicate indexes (%s, %s)\n", tableName, indexSize, index1, index2)
	index1Definition := dbutils.IndexDefinition(index1)
	index2Definition := dbutils.IndexDefinition(index2)
	indexDetail := fmt.Sprintf("\tFirst Index: '%s'\n\tSecond Index: '%s'\n", index1Definition, index2Definition)

	// If Index 2 is unique then kill Index 1
	if strings.Contains(index1Definition, " UNIQUE ") {
		utils.RecordIssue("DuplicateIndex", tableDetail+indexDetail, fmt.Sprintf("\tDROP INDEX %s\n", index1))
		return
	}

	utils.RecordIssue("DuplicateIndex", tableDetail+indexDetail, fmt.Sprintf("\tDROP INDEX %s\n", index1))
}
