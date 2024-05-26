package queue

type Queue interface {
	SendMessage(msg []byte) error
	RecvMessage() ([]byte, error)
	Close() error
}
