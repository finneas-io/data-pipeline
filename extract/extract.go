package extract

import (
	"github.com/finneas-io/premise/api"
	"github.com/finneas-io/premise/bucket"
	"github.com/finneas-io/premise/database"
	"github.com/finneas-io/premise/queue"
)

type Service struct {
	api    *api.Api
	db     database.Database
	bucket bucket.Bucket
	queue  queue.Queue
}

func NewService(a *api.Api, d database.Database, b bucket.Bucket, q queue.Queue) *Service {
	return &Service{api: a, db: d, bucket: b, queue: q}
}

func (s *Service) LoadMissingFilings(cik string) error {

	// find out which filings are missing in the database
	got, err := s.db.GetFilings(cik)
	if err != nil {
		return err
	}
	want, err := s.api.GetFilings(cik)
	if err != nil {
		return err
	}
	for i := len(want) - 1; i >= 0; i-- {
		if got[want[i].Id] {
			want = append(want[:i], want[i+1:]...)
		}
	}

	// get and store the data of the missing filings
	for _, v := range want {
		file, err := s.api.GetFile(cik, v.Id, v.OriginalFile)
		if err != nil {
			return err
		}
		queueErr := s.queue.SendMessage(v.Id + ".htm")
		bucketErr := s.bucket.PutObject(v.Id+".htm", file.Data)
		dbErr := s.db.InsertFiling(&database.Filing{
			Cik:          cik,
			Id:           v.Id,
			OriginalFile: v.OriginalFile,
			Form:         v.Form,
			FilingDate:   v.FilingDate,
			LastModified: file.LastModified,
		})
		if queueErr != nil {
			return queueErr
		}
		if bucketErr != nil {
			return bucketErr
		}
		if dbErr != nil {
			return dbErr
		}
	}

	return nil
}
