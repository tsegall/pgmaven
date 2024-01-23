package plugins

import (
	"fmt"
	"pgmaven/internal/utils"
)

type Command interface {
	Execute(args ...interface{})
}
type CommandBuilder func() Command

type Detector interface {
	Execute(args ...interface{})
	GetIssues() []utils.Issue
}
type DetectorBuilder func() Detector

var commandRegistry map[string]CommandBuilder = map[string]CommandBuilder{
	"CreateTables":   func() Command { return &CreateTables{} },
	"ExecuteQuery":   func() Command { return &QueryRows{} },
	"ResetIndexData": func() Command { return &ResetIndexData{} },
	"SnapShot":       func() Command { return &SnapShot{} },
	"SnapShotTable":  func() Command { return &SnapShotTable{} },
}

var detectorRegistry map[string]DetectorBuilder = map[string]DetectorBuilder{
	"AnalyzeTable":     func() Detector { return &AnalyzeTable{} },
	"AnalyzeTables":    func() Detector { return &AnalyzeTables{} },
	"DuplicateIndexes": func() Detector { return &DuplicateIndexes{} },
}

func NewCommand(name string) (cmd Command, err error) {

	builder, ok := commandRegistry[name]
	if !ok {
		return cmd, fmt.Errorf("Command %v not found", name)
	}
	cmd = builder()
	return
}

func NewDetector(name string) (d Detector, err error) {

	builder, ok := detectorRegistry[name]
	if !ok {
		return d, fmt.Errorf("Command %v not found", name)
	}
	d = builder()
	return
}
