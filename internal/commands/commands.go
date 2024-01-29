package commands

import "fmt"

type Command interface {
	Execute(args ...string)
}

type CommandDetails struct {
	HelpText string
	Builder  func() Command
}

var commandRegistry map[string]CommandDetails = map[string]CommandDetails{
	"CreateTables":   {"Create tables required for tracking activity over time", func() Command { return &CreateTables{} }},
	"QueryRow":       {"Query (single row) to execute across all DBs provided", func() Command { return &QueryRow{} }},
	"QueryRows":      {"Query (multiple rows) to execute across all DBs provided", func() Command { return &QueryRows{} }},
	"Help":           {"Output usage", func() Command { return &Help{} }},
	"ResetIndexData": {"Reset index data", func() Command { return &ResetIndexData{} }},
	"Snapshot":       {"Snapshot statistics tables", func() Command { return &Snapshot{} }},
	"Summary":        {"Status summary", func() Command { return &Summary{} }},
}

func NewCommand(name string) (cmd Command, err error) {

	details, ok := commandRegistry[name]
	if !ok {
		return cmd, fmt.Errorf("Command '%v' not found (use Help to list options)", name)
	}
	cmd = details.Builder()
	return
}
