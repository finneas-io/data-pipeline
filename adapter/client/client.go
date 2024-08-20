package client

import "github.com/finneas-io/data-pipeline/domain/filing"

type Client interface {
	GetCompany(cik string) (*filing.Company, error)
	GetFilings(cik string) ([]*filing.Filing, error)
	GetFile(cik, id, key string) (*filing.File, error)
}
