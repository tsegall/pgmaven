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

func (r *ResetIndexData) Init(context utils.Context, ds *dbutils.DataSource) {
	r.datasource = ds
}

func (r *ResetIndexData) Execute(args ...string) {
	// Reset all Index data
	err, _ := r.datasource.ExecuteQueryRow(`select pg_stat_reset();`, nil)
	if err != nil {
		log.Printf("ERROR: ResetIndexData failed with error: %v\n", err)
	}

	// We have reset the index data so also need to restart our tracking
	r.dropTables()
	new(CreateTables).Execute()
	new(Snapshot).Execute()
}

// DropTables will drop the tables required to track index activity over time.
func (r *ResetIndexData) dropTables() {
	for _, table := range StatsTables {
		r.dropTable(table)
	}
}

func (r *ResetIndexData) dropTable(tableName string) {
	query := fmt.Sprintf("DROP TABLE IF EXISTS pgmaven_%s;", tableName)
	_, err := r.datasource.GetDatabase().Exec(query)
	if err != nil {
		log.Printf("ERROR: dropTable table deletion failed with error: %s\n", err)
	}
}
