package dbinfo

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"
)

var database *sql.DB

func Init(db *sql.DB) {
	database = db
}

func ExecuteQueryRows(query string, processor func(int, []*sql.ColumnType, []interface{})) error {
	var rows *sql.Rows
	var err error

	rows, err = database.Query(query)

	if err != nil {
		fmt.Printf("ERROR: Failed to query database, error: %v\n", err)
		return err
	}
	defer rows.Close()

	columnsTypes, err := rows.ColumnTypes()

	if columnsTypes == nil {
		return nil
	}

	vals := make([]interface{}, len(columnsTypes))
	for i := 0; i < len(columnsTypes); i++ {
		vals[i] = new(interface{})
	}

	rowNumber := 1
	// Process each row
	for rows.Next() {
		err = rows.Scan(vals...)
		if err != nil {
			fmt.Println(err)
			continue
		}
		processor(rowNumber, columnsTypes, vals)
		rowNumber++
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	return nil
}

func Dump(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}) {
	if rowNumber == 1 {
		for i, columnType := range columnTypes {
			if i != 0 {
				fmt.Print("\t")
			}
			fmt.Print(columnType.Name())
		}
		fmt.Println()
	}
	for i := 0; i < len(values); i++ {
		if i != 0 {
			fmt.Print("\t")
		}
		printValue(values[i].(*interface{}))
	}
	fmt.Println()
}

func OutputQueryRows(query string) error {
	return ExecuteQueryRows(query, Dump)
}

func printValue(pval *interface{}) {
	switch v := (*pval).(type) {
	case nil:
		fmt.Print("NULL")
	case bool:
		if v {
			fmt.Print("1")
		} else {
			fmt.Print("0")
		}
	case []byte:
		fmt.Print(string(v))
	case time.Time:
		fmt.Print(v.Format("2006-01-02 15:04:05.999"))
	default:
		fmt.Print(v)
	}
}

func TableList(minRows int) (error, []string) {
	var rows *sql.Rows
	var err error

	if minRows == -1 {
		rows, err = database.Query(`SELECT table_name FROM information_schema.tables where table_schema = 'public' and table_type = 'BASE TABLE' and table_name not ilike 'DBMAVEN_%'`)
	} else {
		rows, err = database.Query(`
			SELECT table_name FROM information_schema.tables, pg_stat_user_tables
			where table_name = relname
		  	  and table_schema = 'public'
		  	  and table_type = 'BASE TABLE'
		  	  and table_name not ilike 'DBMAVEN_%'
		  	  and n_live_tup > $1`, minRows)
	}
	if err != nil {
		fmt.Printf("ERROR: Failed to query database, error: %v\n", err)
		return err, nil
	}
	defer rows.Close()

	ret := make([]string, 0)
	var table_name string

	for rows.Next() {
		err := rows.Scan(&table_name)
		if err != nil {
			log.Printf("ERROR: Failed to get row, error: %v\n", err)
			return err, nil
		}
		ret = append(ret, table_name)
	}
	return nil, ret
}

func ExecuteQueryRow(query string) (error, string) {
	row := database.QueryRow(query)

	var result string
	err := row.Scan(&result)
	if err != nil {
		log.Printf("ERROR: Failed to get row, error: %v\n", err)
		return err, ""
	}

	return nil, result
}

func TableCount(tableName string) (error, string) {
	return ExecuteQueryRow(fmt.Sprintf("SELECT count(*) from %s", tableName))
}

// DuplicateIndexes reports on redundant indexes.
func DuplicateIndexes() {
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
	err := ExecuteQueryRows(duplicateIndexQuery, DuplicateIndexProcessor)
	if err != nil {
		log.Printf("ERROR: DuplicateIndexQuery failed with error: %v\n", err)
	}
}

// IndexDefinition returns the DDL for the named index.
func IndexDefinition(tableName string) string {
	query := fmt.Sprintf(`SELECT pg_get_indexdef('%s'::regclass);`, tableName)
	err, ret := ExecuteQueryRow(query)
	if err != nil {
		log.Printf("ERROR: IndexDefinition failed with error: %v\n", err)
		return ""
	}

	return ret
}

// ResetIndexData will reset the index data (use with care).
func ResetIndexData() {
	// Reset all Index data
	err, _ := ExecuteQueryRow(`select pg_stat_reset();`)
	if err != nil {
		log.Printf("ERROR: ResetIndexData failed with error: %v\n", err)
	}

	// We have reset the index data so also need to restart our tracking
	DropTables()
	CreateTables()
	SnapShot()

	return
}

var statsTables = [...]string{"pg_stat_user_indexes", "pg_statio_user_indexes", "pg_stat_user_tables", "pg_statio_user_tables", "pg_stat_statements"}

func createTable(tableName string) {
	query := fmt.Sprintf("CREATE TABLE pgmaven_%s as table %s with no data;", tableName, tableName)
	_, err := database.Exec(query)
	if err != nil {
		log.Printf("ERROR: CreateTable table creation failed with error: %s\n", err)
	}

	query = fmt.Sprintf("ALTER TABLE pgmaven_%s ADD COLUMN insert_dt TIMESTAMP DEFAULT NOW();", tableName)
	_, err = database.Exec(query)
	if err != nil {
		log.Printf("ERROR: CreateTable alter table failed with error: %s\n", err)
	}
}

// CreateTables will create the tables required to track index activity over time.
func CreateTables() {
	for _, table := range statsTables {
		createTable(table)
	}
}

func dropTable(tableName string) {
	query := fmt.Sprintf("DROP TABLE IF EXISTS pgmaven_%s;", tableName)
	_, err := database.Exec(query)
	if err != nil {
		log.Printf("ERROR: dropTable table deletion failed with error: %s\n", err)
	}
}

// DropTables will drop the tables required to track index activity over time.
func DropTables() {
	for _, table := range statsTables {
		dropTable(table)
	}
}

func SnapShotTable(tableName string) {
	query := fmt.Sprintf("INSERT INTO pgmaven_%s select * from %s;", tableName, tableName)

	_, err := database.Exec(query)
	if err != nil {
		log.Printf("ERROR: SnapShotTable insert failed with error: %s\n", err)
	}
}

func SnapShot() {
	for _, table := range statsTables {
		SnapShotTable(table)
	}
}

// DuplicateIndexProcess is invoked for every row of the Duplicate Index Query.
// The Query returns a row with the following format (tableName, index size, index1, index2) - where index1 and index2 are duplicated.
func DuplicateIndexProcessor(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}) {
	tableName := fmt.Sprintf("%s", *values[0].(*interface{}))
	indexSize := fmt.Sprintf("%s", *values[1].(*interface{}))
	index1 := fmt.Sprintf("%s", *values[2].(*interface{}))
	index2 := fmt.Sprintf("%s", *values[3].(*interface{}))
	fmt.Printf("ISSUE: DuplicateIndex\n")
	fmt.Printf("DETAIL\n")
	fmt.Printf("\tTable: %s, Index Size: %s, Duplicate indexes (%s, %s)\n", tableName, indexSize, index1, index2)
	index1Definition := IndexDefinition(index1)
	index2Definition := IndexDefinition(index2)
	fmt.Printf("\tFirst Index: '%s'\n", index1Definition)
	fmt.Printf("\tSecond Index: '%s'\n", index2Definition)

	// If Index 1 is unique then kill Index 2
	fmt.Printf("SOLUTION\n")
	if strings.Contains(index1Definition, " UNIQUE ") {
		fmt.Printf("\tDROP INDEX %s\n", index2)
		return
	}
	// If Index 2 is unique then kill Index 1
	if strings.Contains(index2Definition, " UNIQUE ") {
		fmt.Printf("\tDROP INDEX %s\n", index1)
		return
	}
	// Neither index is unique - so randomly pick one
	fmt.Printf("\tDROP INDEX %s\n", index2)
}
