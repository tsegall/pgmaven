package commands

import (
	"fmt"
	"log"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type MonitorTerminate struct {
	datasource *dbutils.DataSource
}

func (c *MonitorTerminate) Init(context utils.Context, ds *dbutils.DataSource) {
	c.datasource = ds
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
	query := fmt.Sprintf("DROP TABLE IF EXISTS pgmaven_%s;", tableName)
	_, err := c.datasource.GetDatabase().Exec(query)
	if err != nil {
		log.Printf("ERROR: Database %s, dropTable table deletion failed with error: %s\n", c.datasource.GetDBName(), err)
	}
}
