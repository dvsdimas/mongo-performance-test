package data

type Quote struct {
	Instrument string

	Bid     float32
	BidSize float32
	BidTime int64

	Ask     float32
	AskSize float32
	AskTime int64
}
