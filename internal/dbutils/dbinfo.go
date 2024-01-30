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
)

var (
	database *sql.DB
	options  DBOptions
	dbname   string
)

func Init(db *sql.DB, o DBOptions, d string) {
	database = db
	options = o
	dbname = d
}

func GetDBName() string {
	return dbname
}

func GetSchema() string {
	return options.Schema
}

func GetDatabase() *sql.DB {
	return database
}

func ExecuteQueryRows(query string, queryArgs []any, processor func(int, []*sql.ColumnType, []interface{}, any), processorArg any) error {
	var rows *sql.Rows
	var err error

	rows, err = database.Query(query, queryArgs...)

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
		processor(rowNumber, columnsTypes, vals, processorArg)
		rowNumber++
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	return nil
}

func TableList(minRows int) ([]string, error) {
	var rows *sql.Rows
	var err error

	if minRows == -1 {
		rows, err = database.Query(`SELECT table_name FROM information_schema.tables where table_schema = $1 and table_type = 'BASE TABLE' and table_name not ilike 'PGMAVEN_%'`, options.Schema)
	} else {
		rows, err = database.Query(`
			SELECT table_name FROM information_schema.tables, pg_stat_user_tables
			where table_name = relname
		  	  and table_schema = $1
		  	  and table_type = 'BASE TABLE'
		  	  and table_name not ilike 'PGMAVEN_%'
		  	  and n_live_tup > $2`, options.Schema, minRows)
	}
	if err != nil {
		fmt.Printf("ERROR: Failed to query database, error: %v\n", err)
		return nil, err
	}
	defer rows.Close()

	ret := make([]string, 0)
	var table_name string

	for rows.Next() {
		err := rows.Scan(&table_name)
		if err != nil {
			log.Printf("ERROR: Failed to get row, error: %v\n", err)
			return nil, err
		}
		ret = append(ret, table_name)
	}
	return ret, nil
}

func ExecuteQueryRow(query string) (any, error) {
	row := database.QueryRow(query)

	var result any
	err := row.Scan(&result)
	if err != nil {
		log.Printf("ERROR: Failed to get row, error: %v\n", err)
		return "", err
	}

	return result, nil
}

// IndexDefinition returns the DDL for the named index.
func IndexDefinition(indexName string) string {
	query := fmt.Sprintf(`SELECT pg_get_indexdef('%s'::regclass);`, indexName)
	ret, err := ExecuteQueryRow(query)
	if err != nil {
		log.Printf("ERROR: IndexDefinition failed with error: %v\n", err)
		return ""
	}

	return ret.(string)
}

var StatsTables = [...]string{"pg_stat_user_indexes", "pg_statio_user_indexes", "pg_stat_user_tables", "pg_statio_user_tables", "pg_stat_statements"}
