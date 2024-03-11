package commands

import (
	"fmt"
	"log"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type MonitorTerminate struct {
	datasource *dbutils.DataSource
	context    utils.Context
}

func (c *MonitorTerminate) Init(context utils.Context, ds *dbutils.DataSource) {
	c.datasource = ds
	c.context = context
}

func (c *MonitorTerminate) Execute(args ...string) {
	c.dropTables()
}

// DropTables will drop the tables required to monitor activity
func (c *MonitorTerminate) dropTables() {
	for _, table := range StatsTables {
		c.dropTable(table)
	}
}

func (c *MonitorTerminate) dropTable(tableName string) {
	dropStatement := fmt.Sprintf("DROP TABLE IF EXISTS pgmaven_%s;", tableName)

	if c.context.DryRun || c.context.Verbose {
		log.Println(dropStatement)
	}

	if !c.context.DryRun {
		_, err := c.datasource.GetDatabase().Exec(dropStatement)
		if err != nil {
			log.Printf("ERROR: Database %s, dropTable table deletion failed with error: %s\n", c.datasource.GetDBName(), err)
		}
	}
}
