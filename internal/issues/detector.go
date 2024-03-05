package issues

import (
	"fmt"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type Detector interface {
	Init(context utils.Context, ds *dbutils.DataSource)
	Execute(args ...string)
	GetIssues() []utils.Issue
	GetDurationMS() int64
}

type DetectorDetails struct {
	HelpText string
	Builder  func() Detector
}

var detectorRegistry map[string]DetectorDetails = map[string]DetectorDetails{
	"All":         {"Execute all ", func() Detector { return &AllIssues{} }},
	"Help":        {"Output usage", func() Detector { return &Help{} }},
	"IndexIssues": {"Analyze indexes for issues", func() Detector { return &IndexIssues{} }},
	"QueryIssues": {"Report queries with significant impact on the system", func() Detector { return &QueryIssues{} }},
	"TableIssues": {"Analyze tables for issues", func() Detector { return &TableIssues{} }},
}

func NewDetector(name string) (d Detector, err error) {

	details, ok := detectorRegistry[name]
	if !ok {
		return d, fmt.Errorf("Detector '%v' not found (use Help to list options)", name)
	}
	d = details.Builder()
	return
}
