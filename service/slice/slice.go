package slice

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
	cons   queue.Queue
	prod   queue.Queue
	logger logger.Logger
}

func New(db database.Database, cons queue.Queue, prod queue.Queue, l logger.Logger) *Service {
	return &Service{db: db, cons: cons, prod: prod, logger: l}
}

func (s *Service) SliceFilings() error {

	for {

		// we have to declare variables here because we need to check after the whole iteration
		// if an error was returned and dependendent on that update the fully_stored field in the filing row
		// if we do not declare variables we have a tautological condition when checking the error for nil
		var err error
		var msg []byte
		msg, err = s.cons.RecvMessage()
		if err != nil {
			return err
		}
		fil := &filing.Filing{}
		err = json.Unmarshal(msg, fil)
		if err != nil {
			s.logger.Log(fmt.Sprintf("Queue error: %s", err.Error()))
			continue
		}

		err = fil.LoadTables()
		if err != nil {
			s.logger.Log(fmt.Sprintf("Domain error: %s", err.Error()))
			continue
		}

		for _, t := range fil.Tables {

			var d []byte
			d, err = t.Data.Json()
			if err != nil {
				s.logger.Log(fmt.Sprintf("Domain error: %s", err.Error()))
				continue
			}

			_, err = s.db.InsertTable(fil.Id, t, d)
			if err != nil && err != database.DuplicateErr {
				s.logger.Log(fmt.Sprintf("Database error: %s", err.Error()))
				continue
			}
		}

		// all tables could be inserted into the database
		if err == nil {
			err = s.prod.SendMessage(msg)
			if err != nil {
				s.logger.Log(fmt.Sprintf("Queue error: %s", err.Error()))
			}
		}
	}
}
