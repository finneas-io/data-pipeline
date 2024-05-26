package database

import "github.com/finneas-io/data-pipeline/domain/filing"

type Database interface {
	Close() error
	InsertCompany(cmp *filing.Company) error
	GetCompnies() ([]*filing.Company, error)
	InsertFiling(cik string, fil *filing.Filing) error
	GetFilings(cik string) (map[string]*filing.Filing, error)
	InsertTable(filId string, table *filing.Table, data, comp []byte) error
	GetTables(filId string) ([]*filing.Table, error)
	InsertEdge(edge *filing.Edge) error
}
