package slice

import (
	"encoding/json"

	"github.com/finneas-io/premise/bucket"
	"github.com/finneas-io/premise/database"
	"github.com/finneas-io/premise/queue"
)

type Service struct {
	db     database.Database
	bucket bucket.Bucket
	queue  queue.Queue
}

func NewService(db database.Database, b bucket.Bucket, q queue.Queue) *Service {
	return &Service{db: db, bucket: b, queue: q}
}

type wrapper struct {
	Data [][]string `json:"data"`
}

func (s *Service) Slice(key string) error {
	id, err := parseKey(key)
	if err != nil {
		return err
	}
	data, err := s.bucket.GetObject(key)
	if err != nil {
		return err
	}
	document, err := toHTML(data)
	if err != nil {
		return err
	}
	tables := getTables(document)
	for i, t := range tables {
		result := &wrapper{Data: getTableData(t.node)}
		if !isValid(result.Data) {
			continue
		}
		result.Data = stripStrings(result.Data)
		result.Data = dropRows(result.Data)
		result.Data = dropCols(result.Data)
		if len(result.Data) < 1 {
			continue
		}
		if len(result.Data[0]) < 1 {
			continue
		}
		b, err := json.Marshal(result)
		err = s.db.InsertTable(
			&database.Table{FilingId: id, Index: i, Faktor: t.faktor, Data: string(b)},
		)
		if err != nil {
			return err
		}
		err = s.queue.SendMessage(id)
		if err != nil {
			return err
		}
	}
	return nil
}

func isValid(data [][]string) bool {
	if len(data) < 1 {
		return false
	}
	cols := len(data[0])
	for _, v := range data[1:] {
		if len(v) != cols {
			return false
		}
	}
	return true
}
