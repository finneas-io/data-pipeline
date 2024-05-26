package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/finneas-io/data-pipeline/domain/filing"
	"github.com/jackc/pgx/v5"
)

type postgresDB struct {
	conn *pgx.Conn
}

func New(host, port, name, user, pass string) (*postgresDB, error) {

	conn, err := pgx.Connect(
		context.Background(),
		fmt.Sprintf("postgres://%s:%s@%s:%s/%s", user, pass, host, port, name),
	)
	if err != nil {
		return nil, err
	}
	db := &postgresDB{conn: conn}

	err = db.createTables()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (db *postgresDB) Close() error {
	return db.conn.Close(context.Background())
}

func (db *postgresDB) InsertCompany(cmp *filing.Company) error {

	_, err := db.conn.Exec(
		context.Background(),
		`INSERT INTO company (cik, name) VALUES ($1, $2);`,
		cmp.Cik,
		cmp.Name,
	)
	if err != nil {
		return err
	}

	for _, t := range cmp.Tickers {
		_, err := db.conn.Exec(
			context.Background(),
			`INSERT INTO ticker (value, exchange) VALUES ($1, $2);`,
			t.Value,
			t.Exch,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func nullTime(t time.Time) sql.NullTime {
	if t.IsZero() {
		return sql.NullTime{Valid: false}
	}
	return sql.NullTime{Valid: true, Time: t}
}

func (db *postgresDB) GetCompnies() ([]*filing.Company, error) {
	rows, err := db.conn.Query(context.Background(), `SELECT cik FROM company;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cmps := []*filing.Company{}
	for rows.Next() {
		c := &filing.Company{}
		if err := rows.Scan(&c.Cik); err != nil {
			return nil, err
		}
		cmps = append(cmps, c)
	}

	return cmps, nil
}

func (db *postgresDB) InsertFiling(fil *filing.Filing) error {
	_, err := db.conn.Exec(
		context.Background(),
		`INSERT INTO filing (
			company_cik, 
			id, 
			form, 
			filing_date, 
			last_modified, 
			original_file
		) VALUES ($1, $2, $3, $4, $5, $6);`,
		fil.CmpId,
		fil.Id,
		fil.Form,
		nullTime(fil.FilingDate),
		nullTime(fil.MainFile.LastModified),
		fil.MainFile.Key,
	)
	if err != nil {
		return err
	}
	return nil
}

func (db *postgresDB) GetFilings(cik string) (map[string]*filing.Filing, error) {
	rows, err := db.conn.Query(
		context.Background(),
		`SELECT id FROM filing WHERE filing.company_cik = $1;`,
		cik,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	fils := make(map[string]*filing.Filing)
	for rows.Next() {
		f := &filing.Filing{CmpId: cik}
		if err := rows.Scan(&f.Id); err != nil {
			return nil, err
		}
		fils[f.Id] = f
	}

	return fils, nil
}

func (db *postgresDB) InsertTable(table *filing.Table, data, comp []byte) error {
	_, err := db.conn.Exec(
		context.Background(),
		`INSERT INTO "table" (filing_id, index, faktor, data, compressed_data) VALUES ($1, $2, $3, $4, $5);`,
		table.FilId,
		table.Index,
		table.Faktor,
		data,
		comp,
	)
	if err != nil {
		return err
	}
	return nil
}

func (db *postgresDB) GetTables(id string) ([]*filing.Table, error) {

	rows, err := db.conn.Query(
		context.Background(),
		`SELECT id, compressed_data FROM "table"
			WHERE filing_id = $1 AND compressed_data IS NOT NULL;`,
		id,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := []*filing.Table{}
	for rows.Next() {
		tbl := &filing.Table{}
		data := []byte{}
		if err := rows.Scan(&tbl.Id, &data); err != nil {
			return nil, err
		}
		err = json.Unmarshal(data, &tbl.Data)
		if err != nil {
			return nil, err
		}
		tables = append(tables, tbl)
	}

	return tables, nil
}

func (db *postgresDB) InsertEdge(edge *filing.Edge) error {

	_, err := db.conn.Exec(
		context.Background(),
		`INSERT INTO edge ("from", "to", weight) VALUES ($1, $2, $3);`,
		edge.From.Id,
		edge.To.Id,
		edge.Weight,
	)
	return err
}

func (db *postgresDB) createTables() error {

	_, err := db.conn.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS company (
		cik VARCHAR(10) PRIMARY KEY,
		name VARCHAR(100) NOT NULL
	);`)
	if err != nil {
		return err
	}

	_, err = db.conn.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS ticker (
		id SERIAL PRIMARY KEY,
		company_cik VARCHAR(10) REFERENCES company(cik) ON DELETE CASCADE,
		value VARCHAR(10) UNIQUE NOT NULL,
		exchange VARCHAR(20) DEFAULT NULL
	);`)
	if err != nil {
		return err
	}

	_, err = db.conn.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS filing (
		id VARCHAR(20) PRIMARY KEY,
		company_cik VARCHAR(10) REFERENCES company(cik) ON DELETE CASCADE,
		form VARCHAR(20) NOT NULL,
		filing_date TIMESTAMP DEFAULT NULL,
		last_modified TIMESTAMP DEFAULT NULL,
		original_file VARCHAR(200) NOT NULL
	);`)
	if err != nil {
		return err
	}

	_, err = db.conn.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS "table" (
		id SERIAL PRIMARY KEY,
		filing_id VARCHAR(20) REFERENCES filing(id) ON DELETE CASCADE,
		index INTEGER NOT NULL,
		faktor TEXT DEFAULT NULL,
		data JSONB NOT NULL,
		compressed_data JSONB DEFAULT NULL,
		UNIQUE(filing_id, index)
	);`)
	if err != nil {
		return err
	}

	_, err = db.conn.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS edge (
		"from" SERIAL NOT NULl,
		"to" SERIAL NOT NULl,
		weight INTEGER NOT NULL
	);`)
	if err != nil {
		return err
	}

	return nil
}
