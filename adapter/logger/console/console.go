package console

import "log"

type console struct {
}

func New() *console {
	return &console{}
}

func (c *console) Log(msg string) {
	log.Println(msg)
}
