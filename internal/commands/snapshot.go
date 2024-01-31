package commands

import (
	"pgmaven/internal/dbutils"
)

type Snapshot struct {
	datasource *dbutils.DataSource
}

func (s *Snapshot) Init(ds *dbutils.DataSource) {
	s.datasource = ds
}

func (s *Snapshot) Execute(args ...string) {
	snapShotter := new(SnapshotTable)
	snapShotter.Init(s.datasource)
	for _, table := range StatsTables {
		snapShotter.Execute(table)
	}
}
