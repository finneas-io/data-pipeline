package scylla

import (
	"github.com/finneas-io/premise/database"
	"github.com/gocql/gocql"
)

type scyllaDB struct {
	sess *gocql.Session
}

func NewScyllaDB(host string, port int) (*scyllaDB, error) {
	cluster := gocql.NewCluster(host)
	cluster.Port = port
	sess, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	db := &scyllaDB{sess}
	err = db.createKeyspace()
	if err != nil {
		return nil, err
	}
	err = db.createTables()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (db *scyllaDB) createKeyspace() error {
	return db.sess.Query(
		`CREATE KEYSPACE IF NOT EXISTS statement WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`,
	).Exec()
}

func (db *scyllaDB) createTables() error {
	err := db.createFilingsTable()
	if err != nil {
		return err
	}
	err = db.createCikFilingsIndex()
	if err != nil {
		return err
	}
	err = db.createTablesTable()
	if err != nil {
		return err
	}
	err = db.createGraphsTable()
	if err != nil {
		return err
	}
	return nil
}

func (db *scyllaDB) createFilingsTable() error {
	return db.sess.Query(
		`CREATE TABLE IF NOT EXISTS statement.filing (
			cik VARCHAR,
			id VARCHAR,
			form VARCHAR,
			filing_date TIMESTAMP,
			last_modified TIMESTAMP,
			original_file VARCHAR,
			PRIMARY KEY (cik, filing_date)
		)`,
	).Exec()
}

func (db *scyllaDB) InsertFiling(fil *database.Filing) error {
	err := db.insertCikFilingIndex(fil)
	if err != nil {
		return err
	}
	return db.sess.Query(
		`INSERT INTO statement.filing (cik, id, form, filing_date, last_modified, original_file)
			VALUES (?, ?, ?, ?, ?, ?)`, fil.Cik, fil.Id, fil.Form, fil.FilingDate, fil.LastModified, fil.OriginalFile,
	).Exec()
}

func (db *scyllaDB) GetFilings(cik string) (map[string]bool, error) {
	iter := db.sess.Query(`SELECT id FROM statement.filing WHERE cik = ?`, cik).Iter()
	defer iter.Close()
	m := make(map[string]bool)
	for {
		id := ""
		if !iter.Scan(&id) {
			break
		}
		m[id] = true
	}
	return m, nil
}

func (db *scyllaDB) createCikFilingsIndex() error {
	return db.sess.Query(
		`CREATE TABLE IF NOT EXISTS statement.cik_filing_index (
			id VARCHAR,
			cik VARCHER,
			PRIMARY KEY (id)
		)`,
	).Exec()
}

func (db *scyllaDB) insertCikFilingIndex(fil *database.Filing) error {
	return db.sess.Query(
		`INSERT INTO statement.cik_filing_index (id, cik) VALUES (?, ?)`,
		fil.Id,
		fil.Cik,
	).Exec()
}

func (db *scyllaDB) GetCik(id string) (string, error) {
	cik := ""
	err := db.sess.Query(`SELECT cik FROM statement.cik_filing_index WHERE id = ?`, id).Scan(&cik)
	if err != nil {
		return "", err
	}
	return cik, nil
}

func (db *scyllaDB) createTablesTable() error {
	return db.sess.Query(
		`CREATE TABLE IF NOT EXISTS statement."table" (
			filing_id VARCHAR,
			"index" SMALLINT,
			faktor VARCHAR,
		  data TEXT,
			PRIMARY KEY (filing_id, "index")
		)`,
	).Exec()
}

func (db *scyllaDB) InsertTable(table *database.Table) error {
	return db.sess.Query(
		`INSERT INTO statement."table" (filing_id, "index", faktor, data)
			VALUES (?, ?, ?, ?)`, table.FilingId, table.Index, table.Faktor, table.Data,
	).Exec()
}

func (db *scyllaDB) createGraphsTable() error {
	return db.sess.Query(
		`CREATE TABLE IF NOT EXISTS statement.graph (
			from_id VARCHAR,
			weight int,
			to_id VARCHAR,
			PRIMARY KEY (from_id, weight)
		)`,
	).Exec()
}

func (db *scyllaDB) Close() error {
	// session close doesn't return an error on this driver
	db.sess.Close()
	return nil
}
