package commands

import (
	"fmt"
	"log"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type SnapshotTable struct {
	datasource *dbutils.DataSource
	context    utils.Context
}

func (c *SnapshotTable) Init(context utils.Context, ds *dbutils.DataSource) {
	c.datasource = ds
	c.context = context
}

func (c *SnapshotTable) Execute(args ...string) {
	query := fmt.Sprintf("INSERT INTO pgmaven_%s select * from %s;", args[0], args[0])

	if c.context.DryRun || c.context.Verbose {
		log.Println(query)
	}

	if !c.context.DryRun {
		_, err := c.datasource.GetDatabase().Exec(query)
		if err != nil {
			log.Printf("ERROR: Database '%s', SnapShotTable insert failed with error: %s\n", c.datasource.GetDBName(), err)
		}
	}
}
