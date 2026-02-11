package db

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"grit/fuse"
	"grit/watchdog"
	"io"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	_ "github.com/mattn/go-sqlite3"
)

func (db Database) MakeResourceConsumer() chan fuse.FileData {
	inputChan := make(chan fuse.FileData, 100) // Buffered to prevent deadlock

	go func() {

		var badgetTxn *badger.Txn
		var sqliteTxn *sql.Tx
		var stmt *sql.Stmt

		const MAX = 100
		spaceLeft := MAX

		dog := watchdog.NewWatchdog(100 * time.Millisecond)

		for {
			if badgetTxn == nil {
				dbLogger.Verboseln("Creating new BadgerDB transaction")
				badgetTxn = db.badgerDB.NewTransaction(true)
			}

			if sqliteTxn == nil {
				var err error

				dbLogger.Verboseln("Creating new Sqlite transaction")
				sqliteTxn, err = db.db.BeginTx(context.Background(), nil)
				if err != nil {
					panic(err)
				}

				stmt, err = sqliteTxn.PrepareContext(context.Background(), "INSERT INTO resource (name, object_hash, created_by_task_id) VALUES(?, ?, ?) ON CONFLICT DO NOTHING")
				if err != nil {
					panic(err)
				}
			}

			select {
			case in, ok := <-inputChan:
				if !ok {
					return
				}
				dog.Pet()
				dbLogger.Verboseln("Adding resource to transaction", in.Name)

				parts := strings.Split(in.Name, "/")
				if len(parts) != 2 {
					panic("invalid resource name format")
				}

				taskId := strings.Split(parts[0], "_")[1]
				resourceName := strings.Split(parts[1], "_")[0]

				data, err := io.ReadAll(in.Reader)
				if err != nil {
					panic(err)
				}

				hash := sha256.Sum256(data)

				err = badgetTxn.Set(hash[:], data)
				if err != nil {
					panic(err)
				}

				hs := hex.EncodeToString(hash[:])

				_, err = stmt.Exec(resourceName, hs, taskId)
				if err != nil {
					panic(err)
				}
				spaceLeft--
				dbLogger.Verboseln("Added new resources", in.Name, hs, taskId, spaceLeft)

			case <-dog.Bark:
				dbLogger.Verboseln("Dog barked, flushing transactions")
				spaceLeft = 0
			}

			if spaceLeft == 0 {
				spaceLeft = MAX

				dbLogger.Verboseln("Committing badger transaction")
				err := badgetTxn.Commit()
				if err != nil {
					panic(err)
				}

				badgetTxn = nil

				err = stmt.Close()
				if err != nil {
					panic(err)
				}

				dbLogger.Verboseln("Committing sqlite transaction")
				err = sqliteTxn.Commit()
				if err != nil {
					panic(err)
				}
				sqliteTxn = nil

				dbLogger.Verboseln("Announcing committment")
				db.resourceListener.Broadcast(nil)
			}
		}
	}()

	return inputChan
}
