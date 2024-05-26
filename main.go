package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/finneas-io/data-pipeline/adapter/apiclient"
	"github.com/finneas-io/data-pipeline/adapter/apiclient/httpclient"
	"github.com/finneas-io/data-pipeline/adapter/bucket"
	"github.com/finneas-io/data-pipeline/adapter/bucket/folder"
	"github.com/finneas-io/data-pipeline/adapter/database"
	"github.com/finneas-io/data-pipeline/adapter/database/postgres"
	"github.com/finneas-io/data-pipeline/adapter/logger"
	"github.com/finneas-io/data-pipeline/adapter/logger/console"
	"github.com/finneas-io/data-pipeline/adapter/queue"
	"github.com/finneas-io/data-pipeline/adapter/queue/buffer"
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

	var c apiclient.Client
	c = httpclient.New()

	loadCompanies(db, c, l)

	e := extract.New(db, b, c, extQueue, l)
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

	err = convertMessage(sliQueue, graQueue)
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

func convertMessage(cons queue.Queue, prod queue.Queue) error {
	cmps := make(map[string][]*queue.FilMessage)
	for {
		msgData, err := cons.RecvMessage()
		if err != nil {
			return err
		}
		msg := &queue.FilMessage{}
		err = json.Unmarshal(msgData, msg)
		if err != nil {
			return err
		}

		if cmps[msg.Cik] != nil {
			for _, f := range cmps[msg.Cik] {
				data := &queue.GraphMessage{
					From: msg.Id,
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
			cmps[msg.Cik] = append(cmps[msg.Cik], msg)
		} else {
			cmps[msg.Cik] = []*queue.FilMessage{msg}
		}
	}
}

func loadCompanies(db database.Database, c apiclient.Client, l logger.Logger) {
	data, err := os.ReadFile("./ciks.json")
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
	for _, v := range wrapper.Ciks {
		cmp, err := c.GetCompany(v)
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
