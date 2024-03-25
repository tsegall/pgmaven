package commands

import (
	"log"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type Summary struct {
	datasource *dbutils.DataSource
}

func (c *Summary) Init(context utils.Context, ds *dbutils.DataSource) {
	c.datasource = ds
}

func (c *Summary) Execute(args ...string) {
	isSetupQuery := `
	select count(*) FROM information_schema.tables where table_schema = $1 and table_type = 'BASE TABLE' and table_name ilike 'PGMAVEN_%'
	`
	count, err := c.datasource.ExecuteQueryRow(isSetupQuery, []any{c.datasource.GetSchema()})
	if err != nil {
		log.Fatalf("Summary: Failed to check for existence of pgmaven tables, err: %v\n", err)
	}
	if count.(int64) == 0 {
		log.Fatalf("Summary: No pgmaven tables exist, has MonitorInitialize been run?\n")
	}

	query := `
	select 'ServerVersion' as "Attribute", version() as "Value"
	union all
	select 'ServerStartTime', pg_postmaster_start_time()::text
	union all
	select 'DatabaseName', $1
	union all
	select 'DatabaseSize', pg_size_pretty(pg_database_size(current_database()))
	union all
	select 'pg_stat_statements', CASE WHEN setting ilike('%pg_stat_statements%') THEN 'Enabled' ELSE 'Disabled' END from pg_settings where name = 'shared_preload_libraries'
	union all
	select 'TableCount', count(*)::text FROM information_schema.tables where table_schema = $2 and table_type = 'BASE TABLE' and table_name not ilike 'PGMAVEN_%'
	union all
	select 'IndexCount', count(*)::text from pg_indexes where schemaname = $2
	union all
	select 'TrackingMin', min(insert_dt)::text from pgmaven_pg_stat_statements
	union all
	select 'TrackingMax', max(insert_dt)::text from pgmaven_pg_stat_statements`
	c.datasource.ExecuteQueryRows(query, []any{c.datasource.GetDBName(), c.datasource.GetSchema()}, dump, c)
}
