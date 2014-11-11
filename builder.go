package disruptor

type Builder interface {
	AddConsumerGroup() Builder
	Build() Disruptor
}

func NewBuilder(capacity uint64) Builder {
	return builder{
		capacity: capacity,
		groups:   [][]Consumer{},
		cursors:  []*Cursor{NewCursor()},
	}
}

type builder struct {
	capacity uint64
	groups   [][]Consumer
	cursors  []*Cursor // backing array keeps cursors (with padding) in contiguous memory
}

func (b builder) AddConsumerGroup(consumers ...Consumer) Builder {
	if len(consumers) == 0 {
		return b
	}

	target := make([]Consumer, len(consumers))
	copy(target, consumers)

	for i := 0; i < len(consumers); i++ {
		b.cursors = append(b.cursors, NewCursor())
	}

	b.groups = append(b.groups, target)
	return b
}

func (b builder) Build() Disruptor {
	allReaders := []*Reader{}
	written := b.cursors[0]
	var upstream Barrier = b.cursors[0]
	cursorIndex := 1 // 0 index is reserved for the writer Cursor

	for groupIndex, group := range b.groups {
		groupReaders, groupBarrier := b.buildReaders(groupIndex, cursorIndex, written, upstream)
		for _, item := range groupReaders {
			allReaders = append(allReaders, item)
		}
		upstream = groupBarrier
		cursorIndex += len(group)
	}

	writer := NewWriter(written, upstream, b.capacity)
	return Disruptor{writer: writer, readers: allReaders}
}

func (b builder) buildReaders(consumerIndex, cursorIndex int, written *Cursor, upstream Barrier) ([]*Reader, Barrier) {
	barrierCursors := []*Cursor{}
	readers := []*Reader{}

	for _, consumer := range b.groups[consumerIndex] {
		cursor := b.cursors[cursorIndex]
		barrierCursors = append(barrierCursors, cursor)
		reader := NewReader(cursor, written, upstream, consumer)
		readers = append(readers, reader)
		cursorIndex++
	}

	if len(b.groups[consumerIndex]) == 1 {
		return readers, barrierCursors[0]
	} else {
		return readers, NewCompositeBarrier(barrierCursors...)
	}
}

func NewSharedBuilder(capacity uint64) Builder {
	return sharedBuilder{
		capacity: capacity,
		groups:   [][]Consumer{},
		cursors:  []*Cursor{NewCursor()},
	}
}

type sharedBuilder struct {
	capacity uint64
	groups   [][]Consumer
	cursors  []*Cursor // backing array keeps cursors (with padding) in contiguous memory
}

func (b sharedBuilder) AddConsumerGroup(consumers ...Consumer) Builder {
	if len(consumers) == 0 {
		return b
	}

	target := make([]Consumer, len(consumers))
	copy(target, consumers)

	for i := 0; i < len(consumers); i++ {
		b.cursors = append(b.cursors, NewCursor())
	}

	b.groups = append(b.groups, target)
	return b
}

func (s sharedBuilder) Build() Disruptor {
	allReaders := []*Reader{}
	written := s.cursors[0]
	writerBarrier := NewSharedWriterBarrier(written, s.capacity)
	var upstream Barrier = writerBarrier
	cursorIndex := 1 // 0 index is reserved for the writer Cursor

	for groupIndex, group := range s.groups {
		groupReaders, groupBarrier := s.buildReaders(groupIndex, cursorIndex, written, upstream)
		for _, item := range groupReaders {
			allReaders = append(allReaders, item)
		}
		upstream = groupBarrier
		cursorIndex += len(group)
	}

	writer := NewSharedWriter(writerBarrier, upstream)
	return Disruptor{writer: writer, readers: allReaders}
}

func (s sharedBuilder) buildReaders(consumerIndex, cursorIndex int, written *Cursor, upstream Barrier) ([]*Reader, Barrier) {
	barrierCursors := []*Cursor{}
	readers := []*Reader{}

	for _, consumer := range s.groups[consumerIndex] {
		cursor := s.cursors[cursorIndex]
		barrierCursors = append(barrierCursors, cursor)
		reader := NewReader(cursor, written, upstream, consumer)
		readers = append(readers, reader)
		cursorIndex++
	}

	if len(s.groups[consumerIndex]) == 1 {
		return readers, barrierCursors[0]
	} else {
		return readers, NewCompositeBarrier(barrierCursors...)
	}
}
