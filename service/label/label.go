package label

import (
	"errors"

	"github.com/finneas-io/data-pipeline/adapter/database"
	"github.com/finneas-io/data-pipeline/adapter/logger"
	"github.com/finneas-io/data-pipeline/domain/filing"
	"github.com/google/uuid"
)

type Service interface {
	RandomTable(userId uuid.UUID) (*filing.Company, error)
	CreateLabel(tblId, userId uuid.UUID, label string) error
}

var NoTblLeftErr error = errors.New("No tables left")
var InvalidLabelErr error = errors.New("Invalid label")

type service struct {
	db     database.Database
	logger logger.Logger
	queues map[uuid.UUID]chan *filing.Company
}

func New(db database.Database, l logger.Logger) *service {
	return &service{
		db:     db,
		logger: l,
		queues: make(map[uuid.UUID]chan *filing.Company),
	}
}

func (s *service) RandomTable(userId uuid.UUID) (*filing.Company, error) {

	if s.queues[userId] == nil {
		s.queues[userId] = make(chan *filing.Company, 100)
	}

	if len(s.queues[userId]) < 1 {
		tbls, err := s.db.GetRandomTables(userId)
		if err != nil {
			s.logger.Log(err.Error())
			close(s.queues[userId])
			return nil, err
		}
		if len(tbls) < 1 {
			close(s.queues[userId])
			return nil, NoTblLeftErr
		}
		for _, t := range tbls {
			s.queues[userId] <- t
		}
	}

	return <-s.queues[userId], nil
}

func (s *service) CreateLabel(tblId, userId uuid.UUID, label string) error {

	if label != "cash flow statement" &&
		label != "balance sheet" &&
		label != "financial statement" &&
		label != "other" {
		return InvalidLabelErr
	}

	err := s.db.InsertLabel(tblId, userId, label)
	if err != nil {
		s.logger.Log(err.Error())
		return err
	}

	return nil
}
