package initial

import (
	"encoding/json"
	"fmt"

	"github.com/finneas-io/data-pipeline/adapter/apiclient"
	"github.com/finneas-io/data-pipeline/adapter/bucket"
	"github.com/finneas-io/data-pipeline/adapter/database"
	"github.com/finneas-io/data-pipeline/adapter/logger"
)

type Service struct {
	db     database.Database
	client apiclient.Client
	bucket bucket.Bucket
	logger logger.Logger
}

func New(db database.Database, client apiclient.Client, b bucket.Bucket, l logger.Logger) *Service {
	return &Service{db: db, client: client, bucket: b, logger: l}
}

func (s *Service) InitDatabase() error {
	return s.db.CreateBaseTables()
}

type wrapper struct {
	Ciks []string `json:"ciks"`
}

func (s *Service) LoadCompanies(file string) error {

	data, err := s.bucket.GetObject(file)
	if err != nil {
		return err
	}

	ciks := &wrapper{}
	err = json.Unmarshal(data, ciks)
	if err != nil {
		return err
	}

	for _, cik := range ciks.Ciks {
		cmp, err := s.client.GetCompany(cik)
		if err != nil {
			s.logger.Log(fmt.Sprintf("API Client error: %s", err.Error()))
			continue
		}

		err = s.db.InsertCompany(cmp)
		if err != nil {
			s.logger.Log(fmt.Sprintf("Database error: %s", err.Error()))
		}
	}

	return nil
}
