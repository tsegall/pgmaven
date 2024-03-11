package commands

import (
	"log"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type MonitorReset struct {
	datasource *dbutils.DataSource
	context    utils.Context
}

func (c *MonitorReset) Init(context utils.Context, ds *dbutils.DataSource) {
	c.datasource = ds
	c.context = context
}

func (c *MonitorReset) Execute(args ...string) {
	resetStatement := "select pg_stat_reset();"

	if c.context.DryRun || c.context.Verbose {
		log.Println(resetStatement)
	}

	if !c.context.DryRun {
		// Reset all Index data
		_, err := c.datasource.ExecuteQueryRow(resetStatement, nil)
		if err != nil {
			log.Printf("ERROR: Database %s, MonitorReset failed with error: %v\n", c.datasource.GetDBName(), err)
		}
	}

	// We have reset the index data so also need to restart our tracking
	terminate := new(MonitorTerminate)
	terminate.Init(c.context, c.datasource)
	terminate.Execute()

	initialize := new(MonitorInitialize)
	initialize.Init(c.context, c.datasource)
	initialize.Execute()
}
