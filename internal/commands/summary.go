package commands

import (
	"pgmaven/internal/dbutils"
)

type Summary struct {
	datasource *dbutils.DataSource
}

func (s *Summary) Init(ds *dbutils.DataSource) {
	s.datasource = ds
}

func (s *Summary) Execute(args ...string) {
	query :=
		`SELECT 'TableCount' as "Attribute", count(*)::text as "Value" FROM information_schema.tables where table_schema = $1 and table_type = 'BASE TABLE' and table_name not ilike 'PGMAVEN_%'
	union all
	select 'IndexCount', count(*)::text from pg_indexes where schemaname = $1
	union all
	select 'TrackingMin', min(insert_dt)::text from pgmaven_pg_stat_statements ppss
	union all
	select 'TrackingMax', max(insert_dt)::text from pgmaven_pg_stat_statements ppss`
	s.datasource.ExecuteQueryRows(query, []any{s.datasource.GetSchema()}, dump, s)
}
