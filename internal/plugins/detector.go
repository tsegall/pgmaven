package plugins

import (
	"fmt"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type Detector interface {
	Init(ds *dbutils.DataSource)
	Execute(args ...string)
	GetIssues() []utils.Issue
	GetDurationMS() int64
}
type DetectorDetails struct {
	HelpText string
	Builder  func() Detector
}
type DetectorBuilder func() Detector

var detectorRegistry map[string]DetectorDetails = map[string]DetectorDetails{
	"All":              {"Execute all ", func() Detector { return &AllIssues{} }},
	"AnalyzeTable":     {"Analyze specific table", func() Detector { return &AnalyzeTable{} }},
	"AnalyzeTables":    {"Analyze all tables", func() Detector { return &AnalyzeTables{} }},
	"DuplicateIndexes": {"Check for duplicate indexes", func() Detector { return &DuplicateIndexes{} }},
	"Help":             {"Output usage", func() Detector { return &Help{} }},
	"Queries":          {"Report queries with significant impact on the system", func() Detector { return &Queries{} }},
	"SillyIndexes":     {"Check for silly indexes", func() Detector { return &SillyIndexes{} }},
	"UnusedIndexes":    {"Check for unused indexes", func() Detector { return &UnusedIndexes{} }},
}

func NewDetector(ds *dbutils.DataSource, name string) (d Detector, err error) {

	details, ok := detectorRegistry[name]
	if !ok {
		return d, fmt.Errorf("Detector '%v' not found (use Help to list options)", name)
	}
	d = details.Builder()
	d.Init(ds)
	return
}
