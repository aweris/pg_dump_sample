package database

import "gopkg.in/pg.v4"

func ConnectDB(opts *pg.Options) (*pg.DB, error) {
	db := pg.Connect(opts)
	var model []struct {
		X string
	}
	_, err := db.Query(&model, `SELECT 1 AS x`)
	if err != nil {
		return nil, err
	}
	return db, nil
}
