package commands

import (
	"fmt"
	"log"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type QueryRow struct {
	datasource *dbutils.DataSource
}

func (q *QueryRow) Init(context utils.Context, ds *dbutils.DataSource) {
	q.datasource = ds
}

func (q *QueryRow) Execute(args ...string) {
	s := make([]interface{}, len(args)-1)
	for i := range s {
		s[i] = args[i+1]
	}
	result, err := q.datasource.ExecuteQueryRow(args[0], s)
	if err != nil {
		log.Printf("ERROR: Database: %s, Query '%s' failed with error: %v\n", q.datasource.GetDBName(), args[0], err)
		return
	}
	fmt.Printf("Database: %s, Query '%s', result: %v\n", q.datasource.GetDBName(), args[0], result)
}
