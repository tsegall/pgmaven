package commands

import "fmt"

type Command interface {
	Execute(args ...string)
}
type CommandBuilder func() Command

var commandRegistry map[string]CommandBuilder = map[string]CommandBuilder{
	"CreateTables":   func() Command { return &CreateTables{} },
	"Help":           func() Command { return &Help{} },
	"QueryRows":      func() Command { return &QueryRows{} },
	"QueryRow":       func() Command { return &QueryRow{} },
	"ResetIndexData": func() Command { return &ResetIndexData{} },
	"Snapshot":       func() Command { return &Snapshot{} },
	"SnapshotTable":  func() Command { return &SnapshotTable{} },
	"Summary":        func() Command { return &Summary{} },
}

func NewCommand(name string) (cmd Command, err error) {

	builder, ok := commandRegistry[name]
	if !ok {
		return cmd, fmt.Errorf("Command %v not found", name)
	}
	cmd = builder()
	return
}
