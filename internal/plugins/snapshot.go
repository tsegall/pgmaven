package plugins

import (
	"pgmaven/internal/dbutils"
)

type SnapShot struct {
}

func (command *SnapShot) Execute(args ...interface{}) {
	snapShotter := new(SnapShotTable)
	for _, table := range dbutils.StatsTables {
		snapShotter.Execute(table)
	}
}
