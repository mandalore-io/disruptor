package disruptor

import (
	"sync/atomic"
)

type Writer struct {
	written  *Cursor
	upstream Barrier
	capacity uint64
	previous uint64
	gate     uint64
}

func NewWriter(written *Cursor, upstream Barrier, capacity uint64) Writer {
	assertPowerOfTwo(capacity)

	return Writer{
		upstream: upstream,
		written:  written,
		capacity: capacity,
		previous: InitialSequenceValue,
		gate:     InitialSequenceValue,
	}
}

func assertPowerOfTwo(value uint64) {
	if value > 0 && (value&(value-1)) != 0 {
		// Wikipedia entry: http://bit.ly/1krhaSB
		panic("The ring capacity must be a power of two, e.g. 2, 4, 8, 16, 32, 64, etc.")
	}
}

func (w *Writer) Reserve(count uint64) uint64 {
	w.previous += count
	for w.previous-w.capacity > w.gate {
		w.gate = w.upstream.Read(0)
	}
	return w.previous
}

func (w *Writer) Await(next uint64) {
	for next-w.capacity > w.gate {
		w.gate = w.upstream.Read(0)
	}
}

func (w *Writer) Commit(lower, upper uint64) {
	w.written.sequence = upper
}

type SharedWriter struct {
	written   *Cursor
	upstream  Barrier
	capacity  uint64
	gate      *Cursor
	mask      uint64
	shift     uint8
	committed []uint32
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

func (w *SharedWriter) Reserve(count uint64) uint64 {
	for {
		previous := w.written.Load()
		upper := previous + count

		for upper-w.capacity > w.gate.Load() {
			w.gate.Store(w.upstream.Read(0))
		}

		if atomic.CompareAndSwapUint64(&w.written.sequence, previous, upper) {
			return upper
		}
	}
}

func (w *SharedWriter) Commit(lower, upper uint64) {
	if lower == upper {
		w.committed[upper&w.mask] = uint32(upper >> w.shift)
	} else {
		// working down the array keeps all items in the commit together
		// otherwise the reader(s) could split up the group
		for upper >= lower {
			w.committed[upper&w.mask] = uint32(upper >> w.shift)
			upper--
		}

	}
}
