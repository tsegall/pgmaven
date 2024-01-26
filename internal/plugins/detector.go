package plugins

import (
	"fmt"
	"pgmaven/internal/utils"
)

type Detector interface {
	Execute(args ...string)
	GetIssues() []utils.Issue
}
type DetectorBuilder func() Detector

var detectorRegistry map[string]DetectorBuilder = map[string]DetectorBuilder{
	"All":              func() Detector { return &AllIssues{} },
	"AnalyzeTable":     func() Detector { return &AnalyzeTable{} },
	"AnalyzeTables":    func() Detector { return &AnalyzeTables{} },
	"DuplicateIndexes": func() Detector { return &DuplicateIndexes{} },
	"Help":             func() Detector { return &Help{} },
	"UnusedIndexes":    func() Detector { return &UnusedIndexes{} },
}

func NewDetector(name string) (d Detector, err error) {

	builder, ok := detectorRegistry[name]
	if !ok {
		return d, fmt.Errorf("Command %v not found", name)
	}
	d = builder()
	return
}
