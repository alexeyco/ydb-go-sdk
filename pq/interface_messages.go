package pq

import (
	"context"
	"io"
	"time"
)

type SizeReader interface {
	// Важная часть чтобы можно было экономить память на байтиках.
	// За счет этого можно прочитанные сообщения лениво разжимать, а отправляемые при желании лениво формировать
	io.Reader
	Len() int
}

type MessageData struct { // Данные для записи. Так же эмбедятся в чтение
	SeqNo     int
	CreatedAt time.Time

	Data SizeReader
}

type Message struct {
	Stream    string
	Cluster   string
	Partition int

	MessageData
	CommitOffset

	Source    string
	WrittenAt time.Time
	IP        string

	ctx context.Context // для отслеживания смерти assign
}

var (
	_ CommitableByOffset = Message{}
	_ CommitableByOffset = CommitOffset{}
)

type CommitOffset struct { // Кусочек, необходимый для коммита сообщения
	Offset   uint
	ToOffset uint
	assignID uint
}

func (c CommitOffset) GetCommitOffset() CommitOffset {
	return c
}

func (m Message) Context() context.Context {
	return m.ctx
}

type Batch struct {
	Messages []Message

	CommitOffset // от всех сообщений батча

	ctx context.Context // один на все сообщения
}

func (m Batch) Context() context.Context {
	return m.ctx
}

var (
	_ CommitableByOffset = Batch{}
)
