package archive

import (
	"encoding/json"
	"errors"
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
	queue  queue.Queue
	logger logger.Logger
}

func New(
	db database.Database,
	b bucket.Bucket,
	q queue.Queue,
	l logger.Logger,
) *Service {
	return &Service{db: db, bucket: b, queue: q, logger: l}
}

func (s *Service) StoreFiles() error {

	for {

		var err error = nil

		msg, err := s.queue.RecvMessage()
		if err != nil {
			s.logger.Log(fmt.Sprintf("Queue error: %s", err.Error()))
			continue
		}

		fil := &filing.Filing{}
		err = json.Unmarshal(msg, fil)
		if err != nil {
			s.logger.Log(fmt.Sprintf("Serialization error: %s", err.Error()))
			continue
		}
		if fil.MainFile == nil {
			s.logger.Log(fmt.Sprintf("Serialization error: %s", errors.New("Main file is nil").Error()))
			continue
		}

		err = s.bucket.PutObject(fil.Id+".htm", fil.MainFile.Data)
		if err != nil {
			s.logger.Log(fmt.Sprintf("Bucket error: %s", err.Error()))
			continue
		}

		// if everything went well we can assume that the filing is fully processed
		err = s.db.UpdateStoredFiling(fil.Id)
		if err != nil {
			s.logger.Log(fmt.Sprintf("Database error: %s", err.Error()))
			continue
		}
	}
}
