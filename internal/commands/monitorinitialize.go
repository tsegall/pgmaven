package commands

import (
	"fmt"
	"log"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type MonitorInitialize struct {
	datasource *dbutils.DataSource
	context    utils.Context
}

func (c *MonitorInitialize) Init(context utils.Context, ds *dbutils.DataSource) {
	c.datasource = ds
	c.context = context
}

// MonitorInitialize will create the tables required to track index activity over time.
func (c *MonitorInitialize) Execute(args ...string) {
	for _, table := range StatsTables {
		c.createTable(table)
	}

	snapshotter := new(Snapshot)
	snapshotter.Init(c.context, c.datasource)
	snapshotter.Execute()
}

func (c *MonitorInitialize) createTable(tableName string) {
	stmt := fmt.Sprintf("CREATE TABLE pgmaven_%s as table %s with no data;", tableName, tableName)
	if c.context.DryRun || c.context.Verbose {
		log.Println(stmt)
	}

	if !c.context.DryRun {
		_, err := c.datasource.GetDatabase().Exec(stmt)
		if err != nil {
			log.Printf("ERROR: Database: %s, CreateTable table creation failed, error: %s\n", c.datasource.GetDBName(), err)
		}
	}

	stmt = fmt.Sprintf("ALTER TABLE pgmaven_%s ADD COLUMN insert_dt TIMESTAMP DEFAULT NOW();", tableName)
	if c.context.DryRun || c.context.Verbose {
		log.Println(stmt)
	}

	if !c.context.DryRun {
		_, err := c.datasource.GetDatabase().Exec(stmt)
		if err != nil {
			log.Printf("ERROR: Database: %s, CreateTable alter table failed, error: %s\n", c.datasource.GetDBName(), err)
		}
	}

	stmt = fmt.Sprintf("CREATE INDEX pgmaven_ix_%s_insert_dt ON pgmaven_%s(insert_dt)", tableName, tableName)
	if c.context.DryRun || c.context.Verbose {
		log.Println(stmt)
	}

	if !c.context.DryRun {
		_, err := c.datasource.GetDatabase().Exec(stmt)
		if err != nil {
			log.Printf("ERROR: Database: %s, CreateTable create index failed, error: %s\n", c.datasource.GetDBName(), err)
		}
	}
}
