package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/finneas-io/data-pipeline/adapter/database"
	"github.com/finneas-io/data-pipeline/domain/filing"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type postgresDB struct {
	conn *pgxpool.Pool
}

func New(host, port, name, user, pass string) (*postgresDB, error) {

	conn, err := pgxpool.New(
		context.Background(),
		fmt.Sprintf("postgres://%s:%s@%s:%s/%s", user, pass, host, port, name),
	)
	if err != nil {
		return nil, err
	}

	return &postgresDB{conn: conn}, nil
}

func (db *postgresDB) Close() error {
	db.conn.Close()
	return nil
}

func (db *postgresDB) CreateBaseTables() error {

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
		original_file VARCHAR(200) NOT NULL,
		fully_stored BOOLEAN DEFAULT false
	);`)
	if err != nil {
		return err
	}

	_, err = db.conn.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS "table" (
		id UUID PRIMARY KEY,
		filing_id VARCHAR(20) REFERENCES filing(id) ON DELETE CASCADE,
		header_index INTEGER NOT NULL,
		index INTEGER NOT NULL,
		factor TEXT NOT NULL,
		raw_data TEXT NOT NULL,
		data JSONB NOT NULL,
		label VARCHAR(50) DEFAULT NULL,
		CONSTRAINT unique_filing_id_index UNIQUE(filing_id, index)
	);`)
	if err != nil {
		return err
	}

	_, err = db.conn.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS compressed_table (
		id UUID PRIMARY KEY,
		original_id UUID REFERENCES "table"(id) ON DELETE CASCADE UNIQUE,
		factor VARCHAR(20) NOT NULL,
		header_index INTEGER NOT NULL,
		data JSONB NOT NULL
	);`)
	if err != nil {
		return err
	}

	return nil
}

func (db *postgresDB) InsertCompany(cmp *filing.Company) error {

	_, err := db.conn.Exec(context.Background(), `INSERT INTO company (cik, name) VALUES ($1, $2);`, cmp.Cik, cmp.Name)
	err = errorWrapper(err)
	if err != nil && err != database.DuplicateErr {
		return err
	}

	for _, t := range cmp.Tickers {
		_, err := db.conn.Exec(
			context.Background(),
			`INSERT INTO ticker (company_cik, value, exchange) VALUES ($1, $2, $3);`,
			cmp.Cik,
			t.Value,
			t.Exchange,
		)
		err = errorWrapper(err)
		if err != nil && err != database.DuplicateErr {
			return err
		}
	}

	return nil
}

func (db *postgresDB) GetCompanies() ([]*filing.Company, error) {

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

func (db *postgresDB) InsertFiling(cik string, fil *filing.Filing) error {

	_, err := db.conn.Exec(
		context.Background(),
		`INSERT INTO filing (id, company_cik, form, filing_date, last_modified, original_file) 
			VALUES ($1, $2, $3, $4, $5, $6);`,
		fil.Id,
		cik,
		fil.Form,
		nullTime(fil.FilingDate),
		nullTime(fil.MainFile.LastModified),
		fil.MainFile.Key,
	)

	if err != nil {
		err = errorWrapper(err)
		// filing might already be stored in which case we are happi big time :)
		if err == database.DuplicateErr {
			return nil
		}
		return err
	}

	return nil
}

func (db *postgresDB) UpdateStoredFiling(id string) error {

	_, err := db.conn.Exec(
		context.Background(),
		`UPDATE filing SET fully_stored = true WHERE id = $1;`,
		id,
	)

	return err
}

// we return a map so we can compare which filings are still missing (faster than a list)
func (db *postgresDB) GetFilings(cik string) (map[string]*filing.Filing, error) {

	rows, err := db.conn.Query(
		context.Background(),
		`SELECT id FROM filing WHERE filing.company_cik = $1 AND filing.fully_stored = true;`,
		cik,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	fils := make(map[string]*filing.Filing)
	for rows.Next() {
		f := &filing.Filing{}
		if err := rows.Scan(&f.Id); err != nil {
			return nil, err
		}
		fils[f.Id] = f
	}

	return fils, nil
}

func (db *postgresDB) InsertTable(filId string, table *filing.Table, data []byte) (uuid.UUID, error) {

	id, err := uuid.NewV7()
	if err != nil {
		return uuid.UUID{}, err
	}

	_, err = db.conn.Exec(
		context.Background(),
		`INSERT INTO "table" (id, filing_id, index, factor, header_index, raw_data, data) 
			VALUES ($1, $2, $3, $4, $5, $6, $7);`,
		id,
		filId,
		table.Index,
		table.Factor,
		table.HeadIndex,
		table.RawData,
		data,
	)

	return id, errorWrapper(err)
}

func (db *postgresDB) InsertCompTable(table *filing.Table, data []byte) error {

	id, err := uuid.NewV7()
	if err != nil {
		return err
	}

	_, err = db.conn.Exec(
		context.Background(),
		`INSERT INTO compressed_table (id, original_id, factor, header_index, data) 
			VALUES ($1, $2, $3, $4, $5);`,
		id,
		table.Id,
		table.Factor,
		table.HeadIndex,
		data,
	)

	return errorWrapper(err)
}

func (db *postgresDB) GetTables(limit, page int) ([]*filing.Table, error) {

	rows, err := db.conn.Query(
		context.Background(),
		`SELECT id, index, factor, header_index, data FROM "table" ORDER BY id ASC LIMIT $1 OFFSET $2;`,
		limit,
		page*limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := []*filing.Table{}
	for rows.Next() {
		tbl := &filing.Table{}
		if err := rows.Scan(&tbl.Id, &tbl.Index, &tbl.Factor, &tbl.HeadIndex, &tbl.Data); err != nil {
			return nil, err
		}
		tables = append(tables, tbl)
	}

	return tables, nil
}

// Helper Functions

// to insert null into database timestamps
func nullTime(t time.Time) sql.NullTime {
	if t.IsZero() {
		return sql.NullTime{Valid: false}
	}
	return sql.NullTime{Valid: true, Time: t}
}

// my error wrapper to use custom created error constants defined in database package
func errorWrapper(err error) error {

	// check if error is even present
	if err == nil {
		return nil
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// SQL Error code for violated unique constraint
		if pgErr.Code == "23505" {
			return database.DuplicateErr
		}
	}

	return err
}
