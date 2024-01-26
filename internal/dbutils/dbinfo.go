/*
 * Copyright 2024 Tim Segall
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dbutils

import (
	"database/sql"
	"fmt"
	"log"

	"pgmaven/internal/utils"
)

var (
	database *sql.DB
	options  utils.Options
)

func Init(db *sql.DB, opts utils.Options) {
	database = db
	options = opts
}

func GetDBName() string {
	return options.DBName
}

func GetDatabase() *sql.DB {
	return database
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

func TableList(minRows int) (error, []string) {
	var rows *sql.Rows
	var err error

	if minRows == -1 {
		rows, err = database.Query(`SELECT table_name FROM information_schema.tables where table_schema = 'public' and table_type = 'BASE TABLE' and table_name not ilike 'PGMAVEN_%'`)
	} else {
		rows, err = database.Query(`
			SELECT table_name FROM information_schema.tables, pg_stat_user_tables
			where table_name = relname
		  	  and table_schema = 'public'
		  	  and table_type = 'BASE TABLE'
		  	  and table_name not ilike 'PGMAVEN_%'
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

// IndexDefinition returns the DDL for the named index.
func IndexDefinition(indexName string) string {
	query := fmt.Sprintf(`SELECT pg_get_indexdef('%s'::regclass);`, indexName)
	err, ret := ExecuteQueryRow(query)
	if err != nil {
		log.Printf("ERROR: IndexDefinition failed with error: %v\n", err)
		return ""
	}

	return ret
}

var StatsTables = [...]string{"pg_stat_user_indexes", "pg_statio_user_indexes", "pg_stat_user_tables", "pg_statio_user_tables", "pg_stat_statements"}

func recordIssue(issue string, detail string, solution string) {
	fmt.Printf("ISSUE: %s\n", issue)
	fmt.Printf("DETAIL:\n%s", detail)
	fmt.Printf("SOLUTION:\n%s", solution)
}
