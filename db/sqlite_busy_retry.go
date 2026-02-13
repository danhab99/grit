package db

import (
	"database/sql"
	"errors"
	"strings"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
)

const (
	busyRetryCount = 8
	busyRetryDelay = 25 * time.Millisecond
)

func isSQLiteBusyError(err error) bool {
	if err == nil {
		return false
	}

	var sqliteErr sqlite3.Error
	if errors.As(err, &sqliteErr) {
		if sqliteErr.Code == sqlite3.ErrBusy || sqliteErr.Code == sqlite3.ErrLocked {
			return true
		}
	}
	if errors.Is(err, sqlite3.ErrBusy) || errors.Is(err, sqlite3.ErrLocked) {
		return true
	}
	if strings.Contains(err.Error(), "database is locked") || strings.Contains(err.Error(), "database is busy") {
		return true
	}
	return false
}

func busyBackoff(attempt int) time.Duration {
	return time.Duration(attempt) * busyRetryDelay
}

func (d Database) execWithBusyRetry(query string, args ...any) (sql.Result, error) {
	var err error
	var res sql.Result
	for attempt := 1; attempt <= busyRetryCount; attempt++ {
		res, err = d.db.Exec(query, args...)
		if err == nil {
			return res, nil
		}
		if !isSQLiteBusyError(err) || attempt == busyRetryCount {
			return nil, err
		}
		time.Sleep(busyBackoff(attempt))
	}
	return nil, err
}

func (d Database) queryRowWithBusyRetry(dest []any, query string, args ...any) error {
	var err error
	for attempt := 1; attempt <= busyRetryCount; attempt++ {
		row := d.db.QueryRow(query, args...)
		err = row.Scan(dest...)
		if err == nil {
			return nil
		}
		if !isSQLiteBusyError(err) || attempt == busyRetryCount {
			return err
		}
		time.Sleep(busyBackoff(attempt))
	}
	return err
}
