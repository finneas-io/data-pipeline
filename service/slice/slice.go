package slice

import (
	"encoding/json"
	"fmt"

	"github.com/finneas-io/data-pipeline/adapter/bucket"
	"github.com/finneas-io/data-pipeline/adapter/database"
	"github.com/finneas-io/data-pipeline/adapter/logger"
	"github.com/finneas-io/data-pipeline/adapter/queue"
	"github.com/finneas-io/data-pipeline/domain/filing"
)

type Service struct {
	db     database.Database
	bucket bucket.Bucket
	cons   queue.Queue // the queue from which this service consumes
	prod   queue.Queue // the queue for which this service produces
	logger logger.Logger
}

func New(
	db database.Database,
	b bucket.Bucket,
	cons queue.Queue,
	prod queue.Queue,
	l logger.Logger,
) *Service {
	return &Service{db: db, bucket: b, cons: cons, prod: prod, logger: l}
}

func (s *Service) SliceFilings() error {
	for {
		msg, err := s.cons.RecvMessage()
		if err != nil {
			return err
		}
		fil := &filing.Filing{}
		err = json.Unmarshal(msg, fil)
		if err != nil {
			return err
		}

		data, err := s.bucket.GetObject(fil.Id + ".htm")
		if err != nil {
			return err
		}
		fil.MainFile = &filing.File{Data: data}

		err = fil.LoadTables()
		if err != nil {
			return err
		}

		for _, t := range fil.Tables {
			// some tables are ragged
			comp, _ := t.Data.Compress()
			d, err := t.Data.Json()
			if err != nil {
				s.logger.Log(fmt.Sprintf("Serialization error: %s", err.Error()))
				continue
			}
			c, err := comp.Json()
			if err != nil {
				s.logger.Log(fmt.Sprintf("Serialization error: %s", err.Error()))
				continue
			}
			err = s.db.InsertTable(t, d, c)
			if err != nil {
				s.logger.Log(fmt.Sprintf("Database error: %s", err.Error()))
				continue
			}
		}

		err = s.prod.SendMessage(msg)
		if err != nil {
			s.logger.Log(fmt.Sprintf("Queue error: %s", err.Error()))
		}
	}
}
