package database

import (
	"errors"

	"github.com/finneas-io/data-pipeline/domain/filing"
	"github.com/google/uuid"
)

type Database interface {
	Close() error
	CreateBaseTables() error
	InsertCompany(cmp *filing.Company) error
	UpdateStoredFiling(id string) error
	GetCompanies() ([]*filing.Company, error)
	InsertFiling(cik string, fil *filing.Filing) error
	GetFilings(cik string) (map[string]*filing.Filing, error)
	InsertTable(filId string, table *filing.Table, data []byte) (uuid.UUID, error)
}

var DuplicateErr error = errors.New("Duplicate key error")
var NotFoundErr error = errors.New("Key not found error")
