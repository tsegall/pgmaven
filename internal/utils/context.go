package utils

import "time"

type Context struct {
	DryRun         bool
	Duration       time.Duration
	DurationOffset time.Duration
	Verbose        bool
}
