package join

import (
	"github.com/finneas-io/data-pipeline/adapter/database"
	"github.com/finneas-io/data-pipeline/adapter/logger"
	"github.com/finneas-io/data-pipeline/adapter/queue"
)

type Service struct {
	db     database.Database
	queue  queue.Queue
	logger logger.Logger
}

func New(db database.Database, q queue.Queue, l logger.Logger) *Service {
	return &Service{db: db, queue: q, logger: l}
}

func (s *Service) JoinTables() error {
	for {

	}
}
