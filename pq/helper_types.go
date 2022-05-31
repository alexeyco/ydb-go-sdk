package pq

type CommitableByOffset interface { // Интерфейс, который можно коммитить по оффсету
	GetCommitOffset() CommitOffset
}

type CommitBatch []CommitableByOffset

func (b *CommitBatch) Append(messages ...CommitableByOffset) {
	*b = append(*b, messages...)
}

func (b *CommitBatch) AppendMessages(messages ...Message) {
	*b = append(*b, DeferMessageCommits(messages...)...)
}
