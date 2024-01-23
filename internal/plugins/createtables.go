package plugins

import (
	"fmt"
	"log"
	"pgmaven/internal/dbutils"
)

type CreateTables struct {
}

// CreateTables will create the tables required to track index activity over time.
func (command *CreateTables) Execute(args ...interface{}) {
	for _, table := range dbutils.StatsTables {
		createTable(table)
	}
}

func createTable(tableName string) {
	query := fmt.Sprintf("CREATE TABLE pgmaven_%s as table %s with no data;", tableName, tableName)
	_, err := dbutils.GetDatabase().Exec(query)
	if err != nil {
		log.Printf("ERROR: CreateTable table creation failed with error: %s\n", err)
	}

	query = fmt.Sprintf("ALTER TABLE pgmaven_%s ADD COLUMN insert_dt TIMESTAMP DEFAULT NOW();", tableName)
	_, err = dbutils.GetDatabase().Exec(query)
	if err != nil {
		log.Printf("ERROR: CreateTable alter table failed with error: %s\n", err)
	}
}
