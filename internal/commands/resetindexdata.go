package commands

import (
	"fmt"
	"log"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type ResetIndexData struct {
	datasource *dbutils.DataSource
}

func (c *ResetIndexData) Init(context utils.Context, ds *dbutils.DataSource) {
	c.datasource = ds
}

func (c *ResetIndexData) Execute(args ...string) {
	// Reset all Index data
	err, _ := c.datasource.ExecuteQueryRow(`select pg_stat_reset();`, nil)
	if err != nil {
		log.Printf("ERROR: Database %s, ResetIndexData failed with error: %v\n", c.datasource.GetDBName(), err)
	}

	// We have reset the index data so also need to restart our tracking
	c.dropTables()
	new(CreateTables).Execute()
	new(Snapshot).Execute()
}

// DropTables will drop the tables required to track index activity over time.
func (c *ResetIndexData) dropTables() {
	for _, table := range StatsTables {
		c.dropTable(table)
	}
}

func (c *ResetIndexData) dropTable(tableName string) {
	query := fmt.Sprintf("DROP TABLE IF EXISTS pgmaven_%s;", tableName)
	_, err := c.datasource.GetDatabase().Exec(query)
	if err != nil {
		log.Printf("ERROR: Database %s, dropTable table deletion failed with error: %s\n", c.datasource.GetDBName(), err)
	}
}
