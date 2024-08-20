package create

import (
	"github.com/finneas-io/data-pipeline/adapter/database"
	"github.com/finneas-io/data-pipeline/adapter/logger"
	"github.com/finneas-io/data-pipeline/domain/user"
	"github.com/google/uuid"
)

type service struct {
	db     database.Database
	logger logger.Logger
}

func New(db database.Database, l logger.Logger) *service {
	return &service{db: db, logger: l}
}

func (s *service) CreateUser(username string) error {

	id, err := uuid.NewV7()
	if err != nil {
		return err
	}

	u := &user.User{Username: username, Id: id}
	err = s.db.InsertUser(u)
	if err != nil {
		return err
	}

	return nil
}
