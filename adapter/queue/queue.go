package queue

type Queue interface {
	SendMessage(msg []byte) error
	RecvMessage() ([]byte, error)
	Close() error
}

type FilMessage struct {
	Cik string `json:"cik"`
	Id  string `json:"id"`
}

type GraphMessage struct {
	From string `json:"from"`
	To   string `json:"to"`
}
