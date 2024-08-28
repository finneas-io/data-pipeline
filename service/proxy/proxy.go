package proxy

import (
	"github.com/finneas-io/data-pipeline/adapter/client"
	"github.com/finneas-io/data-pipeline/adapter/logger"
)

type Service interface {
	GetFiling(cik, id, key string) ([]byte, error)
}

type service struct {
	client client.Client
	logger logger.Logger
}

func New(c client.Client, l logger.Logger) *service {
	return &service{client: c, logger: l}
}

func (s *service) GetFiling(cik, id, key string) ([]byte, error) {
	file, err := s.client.GetFile(cik, id, key)
	if err != nil {
		s.logger.Log(err.Error())
		return nil, err
	}
	return file.Data, nil
}
