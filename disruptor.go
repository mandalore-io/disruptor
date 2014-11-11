package disruptor

const (
	MaxSequenceValue     int64 = (1 << 63) - 1
	InitialSequenceValue int64 = -1
	cpuCacheLinePadding        = 7
)

// Interfaces

type Barrier interface {
	Read(uint64) uint64
}

type Consumer interface {
	Consume(lower, upper uint64)
}

type Writer interface {
	Reserve(uint64) uint64
	Await(uint64)
	Commit(uint64, uint64)
}

// Implementation

type Disruptor struct {
	writer  *Writer
	readers []*Reader
}

func (d Disruptor) Writer() *Writer {
	return d.writer
}

func (d Disruptor) Start() {
	for _, item := range d.readers {
		item.Start()
	}
}

func (d Disruptor) Stop() {
	for _, item := range d.readers {
		item.Stop()
	}
}

type Cursor struct {
	sequence int64
	padding  [cpuCacheLinePadding]int64
}

func NewCursor() *Cursor {
	return &Cursor{sequence: InitialSequenceValue}
}

func (c *Cursor) Store(sequence int64) {
	c.sequence = sequence
}

func (c *Cursor) Load() int64 {
	return c.sequence
}

func (c *Cursor) Read(noop int64) int64 {
	return c.sequence
}
