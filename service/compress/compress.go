package compress

import (
	"fmt"

	"github.com/finneas-io/data-pipeline/adapter/database"
	"github.com/finneas-io/data-pipeline/adapter/logger"
)

type Service struct {
	db     database.Database
	logger logger.Logger
}

func New(db database.Database, l logger.Logger) *Service {
	return &Service{db: db, logger: l}
}

func (s *Service) CompressTables() error {
	count := 0

	for {

		tables, err := s.db.GetAllTables(100, count)
		if err != nil {
			s.logger.Log(fmt.Sprintf("Database error: %s", err.Error()))
		}
		if len(tables) < 1 {
			break
		}
		count++

		for _, tbl := range tables {
			err = tbl.Compress()
			if err != nil {
				continue
			}
			d, err := tbl.CompData.Json()
			if err != nil {
				s.logger.Log(fmt.Sprintf("Serialization error: %s", err.Error()))
				continue
			}
			err = s.db.InsertCompTable(tbl, d)
			if err != nil {
				s.logger.Log(fmt.Sprintf("Database error: %s", err.Error()))
			}
		}
	}

	return nil
}
