package buffer

import (
	"sync"

	"github.com/finneas-io/premise/queue"
)

type buffer struct {
	msgs  []string
	mutex sync.Mutex
	cond  *sync.Cond
	drain bool
}

func New() *buffer {
	b := &buffer{drain: false}
	b.cond = sync.NewCond(&b.mutex)
	return b
}

func (q *buffer) SendMessage(msg string) error {
	q.mutex.Lock()
	q.msgs = append(q.msgs, msg)
	q.mutex.Unlock()

	// wake up one waiting consumer
	q.cond.Signal()
	return nil
}

func (q *buffer) ReceiveMessage() (string, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	// wait for a message if buffer is empty
	for len(q.msgs) < 1 {
		if q.drain {
			// no new messages will enter the buffer
			return "", &queue.ErrQueueClosed{}
		}
		q.cond.Wait()
	}

	// consume message and delete it
	msg := q.msgs[len(q.msgs)-1]
	q.msgs = q.msgs[:len(q.msgs)-1]
	return msg, nil
}

func (q *buffer) Drain() {
	q.drain = true
	// wake up all waiting routines to let them recheck the drain flag
	q.cond.Broadcast()
}
