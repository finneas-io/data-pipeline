package graph

import (
	"github.com/finneas-io/premise/database"
)

type Service struct {
	db database.Database
}

func NewService(db database.Database) *Service {
	return &Service{db: db}
}

func (s *Service) Build(id string) error {

	return nil
}
