package main

import (
	"errors"
	"log"
	"sync"

	"github.com/finneas-io/premise/api"
	"github.com/finneas-io/premise/api/web"
	"github.com/finneas-io/premise/bucket"
	"github.com/finneas-io/premise/bucket/folder"
	"github.com/finneas-io/premise/database"
	"github.com/finneas-io/premise/database/scylla"
	"github.com/finneas-io/premise/extract"
	"github.com/finneas-io/premise/graph"
	"github.com/finneas-io/premise/queue"
	"github.com/finneas-io/premise/queue/buffer"
	"github.com/finneas-io/premise/slice"
)

func main() {
	var db database.Database
	db, err := scylla.NewScyllaDB("localhost", 9042)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.Close()
	a := api.New(web.NewClient())
	var sliceQueue queue.Queue
	sliceQueue = buffer.New()
	var graphQueue queue.Queue
	graphQueue = buffer.New()
	var b bucket.Bucket
	b = folder.New("../filings")

	numSlicer := 5
	sliceWg := &sync.WaitGroup{}
	sliceWg.Add(numSlicer)
	for i := 0; i < numSlicer; i++ {
		s := slice.NewService(db, b, graphQueue)
		go slicerWorker(s, sliceQueue, sliceWg)
	}

	numGrapher := 5
	graphWg := &sync.WaitGroup{}
	graphWg.Add(numGrapher)
	for i := 0; i < numGrapher; i++ {
		s := graph.NewService(db)
		go grapherWorker(s, graphQueue, sliceWg)
	}
	extractor := extract.NewService(a, db, b, sliceQueue)
	err = extractor.LoadMissingFilings("0001652044")
	if err != nil {
		log.Fatal(err.Error())
	}

	sliceQueue.Drain()
	sliceWg.Wait()
	graphQueue.Drain()
	graphWg.Wait()

	log.Println("Exited properly")
}

func slicerWorker(s *slice.Service, q queue.Queue, wg *sync.WaitGroup) error {
	defer wg.Done()
	for {
		msg, err := q.ReceiveMessage()
		if err != nil && errors.Is(err, &queue.ErrQueueClosed{}) {
			// queue has been drained
			break
		}
		err = s.Slice(msg)
		if err != nil {
			return err
		}
	}
	return nil
}

func grapherWorker(s *graph.Service, q queue.Queue, wg *sync.WaitGroup) error {
	defer wg.Done()
	for {
		msg, err := q.ReceiveMessage()
		if err != nil && errors.Is(err, &queue.ErrQueueClosed{}) {
			// queue has been drained
			break
		}
		err = s.Build(msg)
		if err != nil {
			return err
		}
	}
	return nil
}
