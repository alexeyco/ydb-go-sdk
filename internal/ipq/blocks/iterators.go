package blocks

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/pq"
)

type MessageIterator interface {
	NextMessage() (data pq.EncodeReader, end bool)
}

type BlockIteratot interface {
	NextBlock() (block *Block, end bool) // Returned block can not be used after next call
}
