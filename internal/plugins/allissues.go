package plugins

import (
	"pgmaven/internal/utils"
)

type AllIssues struct {
	issues []utils.Issue
}

// Run all detection.
func (d *AllIssues) Execute(args ...string) {
	sub, _ := NewDetector("AnalyzeTables")
	sub.Execute()
	d.issues = append(d.issues, sub.GetIssues()...)

	sub, _ = NewDetector("DuplicateIndexes")
	sub.Execute()
	d.issues = append(d.issues, sub.GetIssues()...)

	sub, _ = NewDetector("SillyIndexes")
	sub.Execute()
	d.issues = append(d.issues, sub.GetIssues()...)

	sub, _ = NewDetector("UnusedIndexes")
	sub.Execute()
	d.issues = append(d.issues, sub.GetIssues()...)
}

func (d *AllIssues) GetIssues() []utils.Issue {
	return d.issues
}
