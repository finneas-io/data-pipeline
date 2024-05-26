package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/finneas-io/data-pipeline/adapter/api"
	"github.com/finneas-io/data-pipeline/adapter/api/web"
	"github.com/finneas-io/data-pipeline/adapter/bucket"
	"github.com/finneas-io/data-pipeline/adapter/bucket/folder"
	"github.com/finneas-io/data-pipeline/adapter/database"
	"github.com/finneas-io/data-pipeline/adapter/database/postgres"
	"github.com/finneas-io/data-pipeline/adapter/logger"
	"github.com/finneas-io/data-pipeline/adapter/logger/console"
	"github.com/finneas-io/data-pipeline/adapter/queue"
	"github.com/finneas-io/data-pipeline/adapter/queue/buffer"
	"github.com/finneas-io/data-pipeline/domain/filing"
	"github.com/finneas-io/data-pipeline/service/extract"
	"github.com/finneas-io/data-pipeline/service/graph"
	"github.com/finneas-io/data-pipeline/service/slice"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		panic(err)
	}

	var db database.Database
	db, err = postgres.New(
		envOrPanic("DB_HOST"),
		envOrPanic("DB_PORT"),
		envOrPanic("DB_NAME"),
		envOrPanic("DB_USER"),
		envOrPanic("DB_PASS"),
	)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	var b bucket.Bucket
	b = folder.New(
		envOrPanic("B_PATH"),
	)

	var extQueue queue.Queue
	extQueue = buffer.New()

	var sliQueue queue.Queue
	sliQueue = buffer.New()

	var graQueue queue.Queue
	graQueue = buffer.New()

	var l logger.Logger
	l = console.New()

	var a api.Api
	a = web.New()

	loadCompanies(db, a, l)

	e := extract.New(db, b, a, extQueue, l)
	err = e.LoadFilings()
	if err != nil {
		panic(err)
	}
	extQueue.Close()

	s := slice.New(db, b, extQueue, sliQueue, l)
	err = s.SliceFilings()
	if err != nil {
		l.Log(fmt.Sprintf("Slice Filings: %s", err.Error()))
	}
	sliQueue.Close()

	err = mapFilings(sliQueue, graQueue)
	if err != nil {
		l.Log(fmt.Sprintf("Map Filings: %s", err.Error()))
	}
	graQueue.Close()

	g := graph.New(db, graQueue, l)
	err = g.GraphFilings()
	if err != nil {
		l.Log(fmt.Sprintf("Graph Filings: %s", err.Error()))
	}
}

func mapFilings(cons queue.Queue, prod queue.Queue) error {
	cmps := make(map[string][]*filing.Filing)
	for {
		msg, err := cons.RecvMessage()
		if err != nil {
			return err
		}
		fil := &filing.Filing{}
		err = json.Unmarshal(msg, fil)
		if err != nil {
			return err
		}

		if cmps[fil.CmpId] != nil {
			for _, f := range cmps[fil.CmpId] {
				data := &struct {
					From string `json:"from"`
					To   string `json:"to"`
				}{
					From: fil.Id,
					To:   f.Id,
				}
				b, err := json.Marshal(data)
				if err != nil {
					return err
				}
				err = prod.SendMessage(b)
				if err != nil {
					return err
				}
			}
			cmps[fil.CmpId] = append(cmps[fil.CmpId], fil)
		} else {
			cmps[fil.CmpId] = []*filing.Filing{fil}
		}
	}
}

func loadCompanies(db database.Database, a api.Api, l logger.Logger) {
	data, err := ioutil.ReadFile("./ciks.json")
	if err != nil {
		l.Log(fmt.Sprintf("File error while loading companies: %s", err.Error()))
	}
	wrapper := &struct {
		Ciks []string `json:"ciks"`
	}{}
	err = json.Unmarshal(data, wrapper)
	if err != nil {
		l.Log(fmt.Sprintf("File error while loading companies: %s", err.Error()))
	}
	for _, c := range wrapper.Ciks {
		cmp, err := a.GetCompany(c)
		if err != nil {
			l.Log(fmt.Sprintf("API error while loading companies: %s", err.Error()))
			continue
		}
		err = db.InsertCompany(cmp)
		if err != nil {
			l.Log(fmt.Sprintf("Database error while loading companies: %s", err.Error()))
			continue
		}
	}
}

func envOrPanic(key string) string {
	value := os.Getenv(key)
	if len(value) < 1 {
		panic(errors.New(fmt.Sprintf("Environment variable '%s' missing", key)))
	}
	return value
}
