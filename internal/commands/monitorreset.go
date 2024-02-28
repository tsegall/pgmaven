package commands

import (
	"log"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type MonitorReset struct {
	datasource *dbutils.DataSource
}

func (c *MonitorReset) Init(context utils.Context, ds *dbutils.DataSource) {
	c.datasource = ds
}

func (c *MonitorReset) Execute(args ...string) {
	// Reset all Index data
	err, _ := c.datasource.ExecuteQueryRow(`select pg_stat_reset();`, nil)
	if err != nil {
		log.Printf("ERROR: Database %s, MonitorReset failed with error: %v\n", c.datasource.GetDBName(), err)
	}

	// We have reset the index data so also need to restart our tracking
	new(MonitorTerminate).Execute()
	new(MonitorInitialize).Execute()
}
