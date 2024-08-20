package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/finneas-io/data-pipeline/adapter/database"
	"github.com/finneas-io/data-pipeline/domain/filing"
	"github.com/finneas-io/data-pipeline/domain/user"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type postgres struct {
	conn *pgxpool.Pool
}

func New(host, port, name, user, pass string) (*postgres, error) {

	conn, err := pgxpool.New(
		context.Background(),
		fmt.Sprintf("postgres://%s:%s@%s:%s/%s", user, pass, host, port, name),
	)
	if err != nil {
		return nil, err
	}

	return &postgres{conn: conn}, nil
}

func (db *postgres) Close() error {
	db.conn.Close()
	return nil
}

func (db *postgres) CreateBaseTables() error {

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

	_, err = db.conn.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS "user" (
		id UUID PRIMARY KEY,
		username VARCHAR(100) NOT NULL UNIQUE,
		password VARCHAR(100)
	);`)
	if err != nil {
		return err
	}

	_, err = db.conn.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS "session" (
		token VARCHAR(100) PRIMARY KEY,
		user_id UUID REFERENCES "user"(id) ON DELETE CASCADE UNIQUE,
		expires_at TIMESTAMP NOT NULL
	);`)
	if err != nil {
		return err
	}

	_, err = db.conn.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS table_label (
		table_id UUID REFERENCES "table"(id) ON DELETE CASCADE,
		user_id UUID REFERENCES "user"(id) ON DELETE CASCADE,
		label VARCHAR(100) NOT NULL,
		PRIMARY KEY (table_id, user_id)
	);`)
	if err != nil {
		return err
	}

	return nil
}

func (db *postgres) InsertCompany(cmp *filing.Company) error {

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

func (db *postgres) GetCompanies() ([]*filing.Company, error) {

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

func (db *postgres) InsertFiling(cik string, fil *filing.Filing) error {

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

func (db *postgres) UpdateStoredFiling(id string) error {

	_, err := db.conn.Exec(
		context.Background(),
		`UPDATE filing SET fully_stored = true WHERE id = $1;`,
		id,
	)

	return err
}

// we return a map so we can compare which filings are still missing (faster than a list)
func (db *postgres) GetFilings(cik string) (map[string]*filing.Filing, error) {

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

func (db *postgres) InsertTable(filId string, table *filing.Table, data []byte) (uuid.UUID, error) {

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

func (db *postgres) InsertCompTable(table *filing.Table, data []byte) error {

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

func (db *postgres) GetAllTables(limit, page int) ([]*filing.Table, error) {

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

func (db *postgres) GetCompTables(id string) ([]*filing.Table, error) {

	rows, err := db.conn.Query(
		context.Background(),
		`SELECT compressed_table.id, compressed_table.original_id, "table".index, compressed_table.header_index, 
			compressed_table.factor, compressed_table.data FROM compressed_table, "table"
			WHERE compressed_table.original_id = "table".id AND "table".filing_id = $1
			ORDER BY "table".index ASC;`,
		id,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tbls := []*filing.Table{}
	for rows.Next() {
		tbl := &filing.Table{}
		if err := rows.Scan(
			&tbl.Id,
			&tbl.OriginalId,
			&tbl.Index,
			&tbl.HeadIndex,
			&tbl.Factor,
			&tbl.Data,
		); err != nil {
			return nil, err
		}
		tbls = append(tbls, tbl)
	}

	return tbls, nil
}

func (db *postgres) GetUser(username string) (*user.User, error) {

	user := &user.User{Username: username}

	err := db.conn.QueryRow(
		context.Background(),
		`SELECT "user".id, "user".password FROM "user" WHERE "user".username = $1;`,
		username,
	).Scan(&user.Id, &user.Password)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, database.NotFoundErr
		}
		return nil, err
	}

	return user, nil
}

func (db *postgres) InsertUser(user *user.User) error {

	_, err := db.conn.Exec(
		context.Background(),
		`INSERT INTO "user" (id, username, password) VALUES ($1, $2, $3);`,
		user.Id,
		user.Username,
		user.Password,
	)
	return errorWrapper(err)
}

func (db *postgres) UpdatePassword(user *user.User) error {

	ct, err := db.conn.Exec(
		context.Background(),
		`UPDATE "user" SET password = $2 WHERE id = $1;`,
		user.Id, user.Password,
	)
	if err != nil {
		return err
	}

	if ct.RowsAffected() < 1 {
		return database.NotFoundErr
	}

	return nil
}

func (db *postgres) InsertSession(sess *user.Session) error {

	_, err := db.conn.Exec(
		context.Background(),
		`INSERT INTO "session" (token, user_id, expires_at) VALUES ($1, $2, $3);`,
		sess.Token,
		sess.User.Id,
		sess.ExpiresAt,
	)
	return errorWrapper(err)
}

func (db *postgres) GetSession(token string) (*user.Session, error) {

	u := &user.User{}
	sess := &user.Session{Token: token, User: u}

	err := db.conn.QueryRow(
		context.Background(),
		`SELECT user_id, expires_at FROM "session" WHERE token = $1;`,
		token,
	).Scan(&u.Id, &sess.ExpiresAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, database.NotFoundErr
		}
		return nil, err
	}

	return sess, nil
}

func (db *postgres) DeleteSession(token string) error {

	ct, err := db.conn.Exec(
		context.Background(),
		`DELETE FROM "session" WHERE token = $1;`,
		token,
	)
	if err != nil {
		return err
	}
	if ct.RowsAffected() < 1 {
		return database.NotFoundErr
	}

	return nil
}

func (db *postgres) GetRandomTables(userId uuid.UUID) ([]*filing.Company, error) {

	rows, err := db.conn.Query(
		context.Background(),
		`SELECT company.cik, company.name, filing.id, filing.form, 
			filing.original_file, "table".id, "table".raw_data
			FROM "table"
			JOIN filing ON "table".filing_id = filing.id
			JOIN company ON filing.company_cik = company.cik
			LEFT JOIN table_label ON "table".id = table_label.table_id 
			AND table_label.user_id = $1
			WHERE table_label.table_id IS NULL
			ORDER BY RANDOM() LIMIT 100;`,
		userId,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cmps := []*filing.Company{}
	for rows.Next() {

		cmp := &filing.Company{
			Filings: []*filing.Filing{{
				Tables:   []*filing.Table{{}},
				MainFile: &filing.File{},
			}},
		}
		if err := rows.Scan(
			cmp.Cik,
			cmp.Name,
			cmp.Filings[0].Id,
			cmp.Filings[0].Form,
			cmp.Filings[0].MainFile.Key,
			cmp.Filings[0].Tables[0].Id,
			cmp.Filings[0].Tables[0].RawData,
		); err != nil {
			return nil, err
		}
		cmps = append(cmps, cmp)
	}

	return cmps, nil
}

func (db *postgres) InsertLabel(tblId, userId uuid.UUID, label string) error {

	_, err := db.conn.Exec(
		context.Background(),
		`INSERT INTO "table_label" (table_id, user_id, label) VALUES ($1, $2, $3);`,
		tblId,
		userId,
		label,
	)
	return errorWrapper(err)
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
		// unique_violation
		if pgErr.Code == "23505" {
			return database.DuplicateErr
		}
		// foreign_key_violation
		if pgErr.Code == "23503" {
			return database.InvalidRefErr
		}
	}

	return err
}
