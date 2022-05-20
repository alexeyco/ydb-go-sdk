package persqueue

type CommitableByOffset interface { // Интерфейс, который можно коммитить по оффсету
	GetCommitOffset() CommitOffset
}
