package graph

import (
	"encoding/json"
	"fmt"

	"github.com/finneas-io/data-pipeline/adapter/database"
	"github.com/finneas-io/data-pipeline/adapter/logger"
	"github.com/finneas-io/data-pipeline/adapter/queue"
	"github.com/finneas-io/data-pipeline/domain/filing"
)

type Service struct {
	db     database.Database
	queue  queue.Queue
	logger logger.Logger
}

func New(db database.Database, q queue.Queue, l logger.Logger) *Service {
	return &Service{db: db, queue: q, logger: l}
}

func (s *Service) GraphFilings() error {
	for {

		msgData, err := s.queue.RecvMessage()
		if err != nil {
			return err
		}
		msg := &struct {
			From string `json:"from"`
			To   string `json:"to"`
		}{}
		err = json.Unmarshal(msgData, msg)
		if err != nil {
			return err
		}

		from := &filing.Filing{Id: msg.From}
		tbls, err := s.db.GetTables(from.Id)
		if err != nil {
			return err
		}
		from.Tables = tbls

		to := &filing.Filing{Id: msg.To}
		tbls, err = s.db.GetTables(to.Id)
		if err != nil {
			return err
		}
		to.Tables = tbls

		edges, err := filing.Connect(from, to)
		if err != nil {
			return err
		}

		for _, e := range edges {
			err := s.db.InsertEdge(e)
			if err != nil {
				s.logger.Log(fmt.Sprintf("Database error: %s", err.Error()))
			}
		}

	}
}
