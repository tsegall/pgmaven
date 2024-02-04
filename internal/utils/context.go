package utils

import "time"

type Context struct {
	Duration       time.Duration
	DurationOffset time.Duration
	Verbose        bool
}
