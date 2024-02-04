package commands

import (
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type Snapshot struct {
	datasource *dbutils.DataSource
	context    utils.Context
}

func (s *Snapshot) Init(context utils.Context, ds *dbutils.DataSource) {
	s.datasource = ds
	s.context = context
}

func (s *Snapshot) Execute(args ...string) {
	snapShotter := new(SnapshotTable)
	snapShotter.Init(s.context, s.datasource)
	for _, table := range StatsTables {
		snapShotter.Execute(table)
	}
}
