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

func (c *QueryRow) Init(context utils.Context, ds *dbutils.DataSource) {
	c.datasource = ds
}

func (c *QueryRow) Execute(args ...string) {
	query := utils.OptionallyFromFile(args...)
	s := make([]interface{}, len(args)-1)
	for i := range s {
		s[i] = args[i+1]
	}
	result, err := c.datasource.ExecuteQueryRow(query, s)
	if err != nil {
		log.Printf("ERROR: Database: %s, Query '%s' failed with error: %v\n", c.datasource.GetDBName(), query, err)
		return
	}
	fmt.Printf("Database: %s, Query '%s', result: %v\n", c.datasource.GetDBName(), query, result)
}
