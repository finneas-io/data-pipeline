package database

import "time"

type Database interface {
	Close() error
	InsertFiling(fil *Filing) error
	GetCik(id string) (string, error)
	GetFilings(cik string) (map[string]bool, error)
	InsertTable(table *Table) error
}

type ErrDuplicateKey struct {
}

func (e *ErrDuplicateKey) Error() string {
	return "Key is already taken"
}

type ErrNotFound struct {
	Msg string
}

func (e *ErrNotFound) Error() string {
	return "Entry under specified key does not exist"
}

type Filing struct {
	Cik          string
	Id           string
	FilingDate   time.Time
	LastModified *time.Time
	Form         string
	OriginalFile string
}

type Table struct {
	FilingId string
	Index    int
	Faktor   string
	Data     string
}
