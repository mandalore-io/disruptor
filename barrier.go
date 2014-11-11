package disruptor

import "math"

type CompositeBarrier []*Cursor

func NewCompositeBarrier(upstream ...*Cursor) CompositeBarrier {
	if len(upstream) == 0 {
		panic("At least one upstream cursor is required.")
	}

	cursors := make([]*Cursor, len(upstream))
	copy(cursors, upstream)
	return CompositeBarrier(cursors)
}

func (b CompositeBarrier) Read(noop uint64) uint64 {
	minimum := MaxSequenceValue
	for _, item := range b {
		sequence := item.Load()
		if sequence < minimum {
			minimum = sequence
		}
	}

	return minimum
}

type SharedWriterBarrier struct {
	written   *Cursor
	committed []uint32
	capacity  uint64
	mask      uint64
	shift     uint8
}

func NewSharedWriterBarrier(written *Cursor, capacity uint64) *SharedWriterBarrier {
	assertPowerOfTwo(capacity)

	return &SharedWriterBarrier{
		written:   written,
		committed: prepareCommitBuffer(capacity),
		capacity:  capacity,
		mask:      capacity - 1,
		shift:     uint8(math.Log2(float64(capacity))),
	}
}
func prepareCommitBuffer(capacity uint64) []int32 {
	buffer := make([]uint32, capacity)
	for i := range buffer {
		buffer[i] = uint32(InitialSequenceValue)
	}
	return buffer
}

func (b *SharedWriterBarrier) Read(lower uint64) int64 {
	shift, mask := b.shift, b.mask
	upper := b.written.Load()

	for ; lower <= upper; lower++ {
		if b.committed[lower&mask] != uint32(lower>>shift) {
			return lower - 1
		}
	}

	return upper
}
