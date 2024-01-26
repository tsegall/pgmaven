package commands

import (
	"pgmaven/internal/dbutils"
)

type Snapshot struct {
}

func (command *Snapshot) Execute(args ...string) {
	snapShotter := new(SnapshotTable)
	for _, table := range dbutils.StatsTables {
		snapShotter.Execute(table)
	}
}
