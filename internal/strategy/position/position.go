package position

type Position int8

const (
	LongBullish  Position = 1
	LongBearish  Position = 2
	ShortBullish Position = -2
	ShortBearish Position = -1
	Hold         Position = 0
)
