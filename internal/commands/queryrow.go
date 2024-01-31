package commands

import (
	"fmt"
	"log"
	"pgmaven/internal/dbutils"
)

type QueryRow struct {
	datasource *dbutils.DataSource
}

func (q *QueryRow) Init(ds *dbutils.DataSource) {
	q.datasource = ds
}

func (q *QueryRow) Execute(args ...string) {
	err, result := q.datasource.ExecuteQueryRow(args[0])
	if err != nil {
		log.Printf("ERROR: Database: %s, Query '%s' failed with error: %v\n", q.datasource.GetDBName(), args[0], err)
		return
	}
	fmt.Printf("Database: %s, Query '%s', result: %v\n", q.datasource.GetDBName(), args[0], result)
}
