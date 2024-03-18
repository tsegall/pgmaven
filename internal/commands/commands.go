package commands

import (
	"fmt"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type Command interface {
	Init(context utils.Context, ds *dbutils.DataSource)
	Execute(args ...string)
}

type CommandDetails struct {
	HelpText string
	Builder  func() Command
}

var commandRegistry map[string]CommandDetails = map[string]CommandDetails{
	"Exec":              {"Execute SQL statement across all DBs provided", func() Command { return &Exec{} }},
	"Help":              {"Output usage", func() Command { return &Help{} }},
	"NewActivity":       {"Output New Queries in the specified duration", func() Command { return &NewActivity{} }},
	"QueryRow":          {"Query (single row) to execute across all DBs provided", func() Command { return &QueryRow{} }},
	"QueryRows":         {"Query (multiple rows) to execute across all DBs provided", func() Command { return &QueryRows{} }},
	"MonitorInitialize": {"Initialize infrastructure for activity monitoring", func() Command { return &MonitorInitialize{} }},
	"MonitorReset":      {"Reset activity monitoring data", func() Command { return &MonitorReset{} }},
	"MonitorTerminate":  {"Delete infrastructure for activity monitoring", func() Command { return &MonitorTerminate{} }},
	"Snapshot":          {"Snapshot statistics tables", func() Command { return &Snapshot{} }},
	"Summary":           {"Status summary", func() Command { return &Summary{} }},
}

var StatsTables = [...]string{"pg_stat_user_indexes", "pg_statio_user_indexes", "pg_stat_user_tables", "pg_statio_user_tables", "pg_stat_statements", "pg_stat_activity"}

func NewCommand(name string) (cmd Command, err error) {

	details, ok := commandRegistry[name]
	if !ok {
		return cmd, fmt.Errorf("Command '%v' not found (use Help to list options)", name)
	}
	cmd = details.Builder()
	return
}
