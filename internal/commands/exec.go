package commands

import (
	"fmt"
	"log"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type Exec struct {
	datasource *dbutils.DataSource
}

func (c *Exec) Init(context utils.Context, ds *dbutils.DataSource) {
	c.datasource = ds
}

func (c *Exec) Execute(args ...string) {
	cmd := utils.OptionallyFromFile(args...)
	s := make([]interface{}, len(args)-1)
	for i := range s {
		s[i] = args[i+1]
	}
	result, err := c.datasource.Exec(cmd, s)
	if err != nil {
		log.Printf("ERROR: Database: %s, Exec '%s' failed with error: %v\n", c.datasource.GetDBName(), cmd, err)
		return
	}
	affected, err := result.RowsAffected()
	if err != nil {
		log.Printf("ERROR: Database: %s, result.RowsAffected '%s' failed with error: %v\n", c.datasource.GetDBName(), cmd, err)
		return
	}
	fmt.Printf("Database: %s, Exec '%s', rows affected: %d\n", c.datasource.GetDBName(), cmd, affected)
}
