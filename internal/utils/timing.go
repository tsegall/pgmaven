package utils

type Timing struct {
	durationMS int64
}

func (t *Timing) SetDurationMS(duration int64) {
	t.durationMS = duration
}
func (t *Timing) GetDurationMS() int64 {
	return t.durationMS
}
