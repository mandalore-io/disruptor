package disruptor

import (
	"sync/atomic"
)

type Writer struct {
	written  *Cursor
	upstream Barrier
	capacity int64
	previous int64
	gate     int64
}

func NewWriter(written *Cursor, upstream Barrier, capacity int64) Writer {
	assertPowerOfTwo(capacity)

	return Writer{
		upstream: upstream,
		written:  written,
		capacity: capacity,
		previous: InitialSequenceValue,
		gate:     InitialSequenceValue,
	}
}

func assertPowerOfTwo(value int64) {
	if value > 0 && (value&(value-1)) != 0 {
		// Wikipedia entry: http://bit.ly/1krhaSB
		panic("The ring capacity must be a power of two, e.g. 2, 4, 8, 16, 32, 64, etc.")
	}
}

func (w *Writer) Reserve(count int64) int64 {
	w.previous += count
	for w.previous-w.capacity > w.gate {
		w.gate = w.upstream.Read(0)
	}
	return w.previous
}

func (w *Writer) Await(next int64) {
	for next-w.capacity > w.gate {
		w.gate = w.upstream.Read(0)
	}
}

func (w *Writer) Commit(lower, upper int64) {
	w.written.sequence = upper
}

type SharedWriter struct {
	written   *Cursor
	upstream  Barrier
	capacity  int64
	gate      *Cursor
	mask      int64
	shift     uint8
	committed []int32
}

func NewSharedWriter(write *SharedWriterBarrier, upstream Barrier) SharedWriter {
	return SharedWriter{
		written:   write.written,
		upstream:  upstream,
		capacity:  write.capacity,
		gate:      NewCursor(),
		mask:      write.mask,
		shift:     write.shift,
		committed: write.committed,
	}
}

func (w *SharedWriter) Reserve(count int64) int64 {
	for {
		previous := w.written.Load()
		upper := previous + count

		for upper-w.capacity > w.gate.Load() {
			w.gate.Store(w.upstream.Read(0))
		}

		if atomic.CompareAndSwapInt64(&w.written.sequence, previous, upper) {
			return upper
		}
	}
}

func (w *SharedWriter) Commit(lower, upper int64) {
	if lower == upper {
		w.committed[upper&w.mask] = int32(upper >> w.shift)
	} else {
		// working down the array keeps all items in the commit together
		// otherwise the reader(s) could split up the group
		for upper >= lower {
			w.committed[upper&w.mask] = int32(upper >> w.shift)
			upper--
		}
	}
}
