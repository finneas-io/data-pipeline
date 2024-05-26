package queue

type Queue interface {
	SendMessage(msg []byte) error
	RecvMessage() ([]byte, error)
	Close() error
}

type Message struct {
	Cik string `json:"cik"`
	Id  string `json:"id"`
}
