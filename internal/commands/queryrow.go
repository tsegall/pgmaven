package commands

import (
	"fmt"
	"log"
	"pgmaven/internal/dbutils"
)

type QueryRow struct {
}

func (command *QueryRow) Execute(args ...string) {
	err, result := dbutils.ExecuteQueryRow(args[0])
	if err != nil {
		log.Printf("ERROR: Database: %s, Query '%s' failed with error: %v\n", dbutils.GetDBName(), args[0], err)
		return
	}
	fmt.Printf("Database: %s, Query '%s', result: %s\n", dbutils.GetDBName(), args[0], result)
}
