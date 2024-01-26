package commands

import (
	"fmt"
	"log"
	"pgmaven/internal/dbutils"
)

type ResetIndexData struct {
}

func (command *ResetIndexData) Execute(args ...string) {
	// Reset all Index data
	err, _ := dbutils.ExecuteQueryRow(`select pg_stat_reset();`)
	if err != nil {
		log.Printf("ERROR: ResetIndexData failed with error: %v\n", err)
	}

	// We have reset the index data so also need to restart our tracking
	dropTables()
	new(CreateTables).Execute()
	new(Snapshot).Execute()

	return
}

// DropTables will drop the tables required to track index activity over time.
func dropTables() {
	for _, table := range dbutils.StatsTables {
		dropTable(table)
	}
}

func dropTable(tableName string) {
	query := fmt.Sprintf("DROP TABLE IF EXISTS pgmaven_%s;", tableName)
	_, err := dbutils.GetDatabase().Exec(query)
	if err != nil {
		log.Printf("ERROR: dropTable table deletion failed with error: %s\n", err)
	}
}
