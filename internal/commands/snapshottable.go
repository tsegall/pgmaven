package commands

import (
	"fmt"
	"log"
	"pgmaven/internal/dbutils"
)

type SnapshotTable struct {
}

func (command *SnapshotTable) Execute(args ...string) {
	query := fmt.Sprintf("INSERT INTO pgmaven_%s select * from %s;", args[0], args[0])

	_, err := dbutils.GetDatabase().Exec(query)
	if err != nil {
		log.Printf("ERROR: SnapShotTable insert failed with error: %s\n", err)
	}
}
