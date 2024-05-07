package queue

type Queue interface {
	SendMessage(msg string) error
	ReceiveMessage() (string, error)
	Drain()
}

type ErrQueueClosed struct {
}

func (e *ErrQueueClosed) Error() string {
	return "Queue has been closed"
}
