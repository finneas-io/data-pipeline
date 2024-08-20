package database

import (
	"errors"

	"github.com/finneas-io/data-pipeline/domain/filing"
	"github.com/finneas-io/data-pipeline/domain/user"
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
	InsertCompTable(table *filing.Table, data []byte) error
	GetAllTables(limit, page int) ([]*filing.Table, error)
	GetCompTables(id string) ([]*filing.Table, error)
	GetUser(username string) (*user.User, error)
	InsertUser(user *user.User) error
	UpdatePassword(user *user.User) error
	InsertSession(sess *user.Session) error
	DeleteSession(token string) error
	GetSession(token string) (*user.Session, error)
	GetRandomTables(userId uuid.UUID) ([]*filing.Company, error)
	InsertLabel(tblId, userId uuid.UUID, label string) error
}

var DuplicateErr error = errors.New("Duplicate key error")
var InvalidRefErr error = errors.New("Foreign key reference does not exist")
var NotFoundErr error = errors.New("Key not found error")
