package commands

import (
	"fmt"
	"log"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type SnapshotTable struct {
	datasource *dbutils.DataSource
}

func (s *SnapshotTable) Init(context utils.Context, ds *dbutils.DataSource) {
	s.datasource = ds
}

func (s *SnapshotTable) Execute(args ...string) {
	query := fmt.Sprintf("INSERT INTO pgmaven_%s select * from %s;", args[0], args[0])

	_, err := s.datasource.GetDatabase().Exec(query)
	if err != nil {
		log.Printf("ERROR: SnapShotTable insert failed with error: %s\n", err)
	}
}
