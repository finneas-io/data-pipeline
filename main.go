package main

import (
	"errors"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/finneas-io/data-pipeline/adapter/bucket"
	"github.com/finneas-io/data-pipeline/adapter/bucket/folder"
	"github.com/finneas-io/data-pipeline/adapter/bucket/vault"
	"github.com/finneas-io/data-pipeline/adapter/client"
	"github.com/finneas-io/data-pipeline/adapter/client/httpclnt"
	"github.com/finneas-io/data-pipeline/adapter/database"
	"github.com/finneas-io/data-pipeline/adapter/database/postgres"
	"github.com/finneas-io/data-pipeline/adapter/logger"
	"github.com/finneas-io/data-pipeline/adapter/logger/console"
	"github.com/finneas-io/data-pipeline/adapter/queue"
	"github.com/finneas-io/data-pipeline/adapter/queue/buffer"
	"github.com/finneas-io/data-pipeline/adapter/server/httpserv"
	"github.com/finneas-io/data-pipeline/service/archive"
	"github.com/finneas-io/data-pipeline/service/auth"
	"github.com/finneas-io/data-pipeline/service/compress"
	"github.com/finneas-io/data-pipeline/service/create"
	"github.com/finneas-io/data-pipeline/service/extract"
	"github.com/finneas-io/data-pipeline/service/initial"
	"github.com/finneas-io/data-pipeline/service/label"
	"github.com/finneas-io/data-pipeline/service/slice"
	"github.com/joho/godotenv"
)

func main() {

	if len(os.Args) < 2 {
		panic(errors.New("One command line argument must be passed"))
	}

	godotenv.Load()
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")
	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASS")

	// adapter which are needed in all commands
	var db database.Database
	db, err := postgres.New(host, port, name, user, pass)
	if err != nil {
		panic(err)
	}
	var l logger.Logger = console.New()

	if os.Args[1] == "init" {
		var client client.Client = httpclnt.New()
		var root bucket.Bucket = folder.New(".")

		initService := initial.New(db, client, root, l)

		err = initService.InitDatabase()
		if err != nil {
			panic(err)
		}

		err = initService.LoadCompanies("ciks.json")
		if err != nil {
			panic(err)
		}
	}

	if os.Args[1] == "load" {
		var client client.Client = httpclnt.New()
		var exctQueue queue.Queue = buffer.New()
		var slicQueue queue.Queue = buffer.New()

		exctService := extract.New(db, client, exctQueue, l)

		go func() {
			err = exctService.LoadFilings()
			if err != nil {
				log.Println(err.Error())
			}
		}()

		slicService := slice.New(db, exctQueue, slicQueue, l)

		go func() {
			err = slicService.SliceFilings()
			if err != nil {
				log.Println(err.Error())
			}
		}()

		region := os.Getenv("REGION") // region for aws
		sess, err := session.NewSession(&aws.Config{
			Region: aws.String(region),
		})
		if err != nil {
			panic(err)
		}

		archName := os.Getenv("ARCHIVE") // name of the glacier vault
		var a bucket.Bucket = vault.New(sess, archName)

		archService := archive.New(db, a, slicQueue, l)

		err = archService.StoreFiles()
		if err != nil {
			log.Println(err.Error())
		}
	}

	if os.Args[1] == "compress" {
		compService := compress.New(db, l)
		err := compService.CompressTables()
		if err != nil {
			panic(err)
		}
	}

	if os.Args[1] == "create" {
		if len(os.Args) != 3 {
			panic(errors.New("Exactly one additional argument is required for this command"))
		}
		crteService := create.New(db, l)
		err := crteService.CreateUser(os.Args[2])
		if err != nil {
			panic(err)
		}
	}

	if os.Args[1] == "webserver" {
		panic(httpserv.New(8000, auth.New(db, l), label.New(db, l)).Listen())
	}
}
