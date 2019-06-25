package data

type Quote struct {
	Id         uint64
	Instrument string
	Bid        float64
	Ask        float64
	Time       int64
}
