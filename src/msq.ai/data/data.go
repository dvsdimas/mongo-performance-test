package data

type Quote struct {
	Source     byte
	Instrument string
	Bid        float64
	Ask        float64
	Time       int64
}
