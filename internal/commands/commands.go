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
	"CreateTables":   {"Create tables required for tracking activity over time", func() Command { return &CreateTables{} }},
	"Exec":           {"Execute SQL statement across all DBs provided", func() Command { return &Exec{} }},
	"Help":           {"Output usage", func() Command { return &Help{} }},
	"NewActivity":    {"Output New Queries in the specified duration", func() Command { return &NewActivity{} }},
	"QueryRow":       {"Query (single row) to execute across all DBs provided", func() Command { return &QueryRow{} }},
	"QueryRows":      {"Query (multiple rows) to execute across all DBs provided", func() Command { return &QueryRows{} }},
	"ResetIndexData": {"Reset index data", func() Command { return &ResetIndexData{} }},
	"Snapshot":       {"Snapshot statistics tables", func() Command { return &Snapshot{} }},
	"Summary":        {"Status summary", func() Command { return &Summary{} }},
}

var StatsTables = [...]string{"pg_stat_user_indexes", "pg_statio_user_indexes", "pg_stat_user_tables", "pg_statio_user_tables", "pg_stat_statements"}

func NewCommand(name string) (cmd Command, err error) {

	details, ok := commandRegistry[name]
	if !ok {
		return cmd, fmt.Errorf("Command '%v' not found (use Help to list options)", name)
	}
	cmd = details.Builder()
	return
}
