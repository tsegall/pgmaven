package commands

import (
	"log"
)

type Summary struct {
}

// CreateTables will create the tables required to track index activity over time.
func (c *Summary) Execute(args ...string) {
	query :=
		`SELECT 'TableCount' as "Attribute", count(*)::text as "Value" FROM information_schema.tables where table_schema = 'public' and table_type = 'BASE TABLE' and table_name not ilike 'PGMAVEN_%'
	union all
	select 'IndexCount', count(*)::text from pg_indexes where schemaname = 'public'
	union all
	select 'TrackingMin', min(insert_dt)::text from pgmaven_pg_stat_statements ppss
	union all
	select 'TrackingMax', max(insert_dt)::text from pgmaven_pg_stat_statements ppss`
	command, err := NewCommand("QueryRows")
	if err != nil {
		log.Println("ERROR: Failed to locate command\n", err)
		return
	}
	command.Execute(query)
}
