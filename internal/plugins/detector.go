package plugins

import "fmt"

type Command interface {
	Execute(args ...interface{})
}

type CommandBuilder func() Command

var commandRegistry map[string]CommandBuilder = map[string]CommandBuilder{
	"AnalyzeTable":     func() Command { return &AnalyzeTable{} },
	"CreateTables":     func() Command { return &CreateTables{} },
	"DuplicateIndexes": func() Command { return &DuplicateIndexes{} },
	"ExecuteQuery":     func() Command { return &QueryRows{} },
	"ResetIndexData":   func() Command { return &ResetIndexData{} },
	"SnapShot":         func() Command { return &SnapShot{} },
	"SnapShotTable":    func() Command { return &SnapShotTable{} },
}

func NewCommand(name string) (cmd Command, err error) {

	builder, ok := commandRegistry[name]
	if !ok {
		return cmd, fmt.Errorf("Command %v not found", name)
	}
	cmd = builder()
	return
}
