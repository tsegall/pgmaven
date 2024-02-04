package commands

import (
	"fmt"
	"log"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type CreateTables struct {
	datasource *dbutils.DataSource
}

func (c *CreateTables) Init(context utils.Context, ds *dbutils.DataSource) {
	c.datasource = ds
}

// CreateTables will create the tables required to track index activity over time.
func (c *CreateTables) Execute(args ...string) {
	for _, table := range StatsTables {
		c.createTable(table)
	}
}

func (c *CreateTables) createTable(tableName string) {
	query := fmt.Sprintf("CREATE TABLE pgmaven_%s as table %s with no data;", tableName, tableName)
	_, err := c.datasource.GetDatabase().Exec(query)
	if err != nil {
		log.Printf("ERROR: CreateTable table creation failed with error: %s\n", err)
	}

	query = fmt.Sprintf("ALTER TABLE pgmaven_%s ADD COLUMN insert_dt TIMESTAMP DEFAULT NOW();", tableName)
	_, err = c.datasource.GetDatabase().Exec(query)
	if err != nil {
		log.Printf("ERROR: CreateTable alter table failed with error: %s\n", err)
	}
}
