package commands

import (
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type Snapshot struct {
	datasource *dbutils.DataSource
	context    utils.Context
}

func (c *Snapshot) Init(context utils.Context, ds *dbutils.DataSource) {
	c.datasource = ds
	c.context = context
}

func (c *Snapshot) Execute(args ...string) {
	snapShotter := new(SnapshotTable)
	snapShotter.Init(c.context, c.datasource)
	for _, table := range StatsTables {
		snapShotter.Execute(table)
	}
}
