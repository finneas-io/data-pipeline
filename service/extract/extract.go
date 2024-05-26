package extract

import (
	"encoding/json"
	"fmt"

	"github.com/finneas-io/data-pipeline/adapter/apiclient"
	"github.com/finneas-io/data-pipeline/adapter/bucket"
	"github.com/finneas-io/data-pipeline/adapter/database"
	"github.com/finneas-io/data-pipeline/adapter/logger"
	"github.com/finneas-io/data-pipeline/adapter/queue"
	"github.com/finneas-io/data-pipeline/domain/filing"
)

type Service struct {
	db     database.Database
	bucket bucket.Bucket
	client apiclient.Client
	queue  queue.Queue
	logger logger.Logger
}

func New(
	db database.Database,
	b bucket.Bucket,
	c apiclient.Client,
	q queue.Queue,
	l logger.Logger,
) *Service {
	return &Service{db: db, bucket: b, client: c, queue: q, logger: l}
}

func (s *Service) LoadFilings() error {
	cmps, err := s.db.GetCompnies()
	if err != nil {
		return err
	}

	for _, cmp := range cmps {

		// filings in the database returned as look up map
		got, err := s.db.GetFilings(cmp.Cik)
		if err != nil {
			return err
		}

		// all possible filings received from the API
		all, err := s.client.GetFilings(cmp.Cik)
		if err != nil {
			return err
		}

		want := []*filing.Filing{}
		for _, v := range all {

			// check if filing is already in database
			if got[v.Id] != nil {
				continue
			}

			v.MainFile, err = s.client.GetFile(cmp.Cik, v.Id, v.MainFile.Key)
			if err != nil {
				s.logger.Log(fmt.Sprintf("API Client error: %s", err.Error()))
				continue
			}

			want = append(want, v)
		}

		// load missing filings into database, bucket and queue
		for _, v := range want {
			msg := &queue.FilMessage{Cik: cmp.Cik, Id: v.Id}
			b, err := json.Marshal(msg)
			if err != nil {
				s.logger.Log(fmt.Sprintf("Serialization error: %s", err.Error()))
				continue
			}
			err = s.queue.SendMessage(b)
			if err != nil {
				s.logger.Log(fmt.Sprintf("Queue error: %s", err.Error()))
				continue
			}
			err = s.bucket.PutObject(v.Id+".htm", v.MainFile.Data)
			if err != nil {
				s.logger.Log(fmt.Sprintf("Bucket error: %s", err.Error()))
				continue
			}
			err = s.db.InsertFiling(cmp.Cik, v)
			if err != nil {
				s.logger.Log(fmt.Sprintf("Database error: %s", err.Error()))
				continue
			}
		}
	}

	return nil
}
