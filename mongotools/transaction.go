package mongotools

import (
	"errors"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
)

// A Transaction manages the lifetime of a multi-statement transaction.
// If the server is running basic MongoDB instead of TokuMX, this will be a no-op.
//
// Example:
//
//     txn := mongotools.Transaction{DB: session.DB("admin")}
//     if err := txn.Begin("mvcc"); err != nil {
//         log.Fatal(err)
//     }
//     defer txn.Close()
//     // ...
//     if success {
//         if err := txn.Commit(); err != nil {
//             log.Fatal(err)
//         }
//     }
type Transaction struct {
	DB   *mgo.Database
	live bool
}

// Begin starts the transaction and accepts an optional isolation parameter.
// The valid values for isolation are "mvcc" (the default), "serializable", and "readUncommitted".
// After a successful begin, the application is responsible for calling Close().
func (txn *Transaction) Begin(isos ...string) error {
	if !IsTokuMX(txn.DB) {
		return nil
	}

	cmd := bson.M{"beginTransaction": 1}
	if len(isos) == 1 {
		switch isos[0] {
		case "serializable":
			fallthrough
		case "mvcc":
			fallthrough
		case "readUncommitted":
			cmd[isos[0]] = 1
		default:
			return errors.New("invalid isolation type")
		}
	}

	var res bson.M
	if err := txn.DB.Run(cmd, &res); err != nil {
		return err
	}
	txn.live = true
	return nil
}

// Commit commits the current transaction.
func (txn *Transaction) Commit() error {
	var res bson.M
	if err := txn.DB.Run(bson.M{"commitTransaction": 1}, &res); err != nil {
		return err
	}
	txn.live = false
	return nil
}

// Rollback rolls the current transaction back.
func (txn *Transaction) Rollback() error {
	var res bson.M
	if err := txn.DB.Run(bson.M{"rollbackTransaction": 1}, &res); err != nil {
		return err
	}
	txn.live = false
	return nil
}

// Close checks whether the transaction has already been committed or rolled back, and if not, it calls Rollback().
func (txn *Transaction) Close() {
	if txn.live {
		if err := txn.Rollback(); err != nil {
			log.Fatal(err)
		}
		txn.live = false
	}
}
