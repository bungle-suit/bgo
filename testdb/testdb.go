package testdb

import (
	"context"
	"io"
	"testing"

	"github.com/bungle-suit/bgo"
	"github.com/bungle-suit/tt"
	"go.mongodb.org/mongo-driver/mongo"
)

// TestDB create a connection to mongodb, and delete the database in reset. Also
// provide mongodb operation methods, so we do not need to deal with returned error.
// *mgo.Databae is nested in, so all Database method con be used directly from TestDB, such as:
//
//   db := testdb.New("blah_test")
//   db.C("tbl").Insert(...
//
// Instead of:
//
//   db.Session.DB("").C("tbl").Insert(...
type TestDB struct {
	*mongo.Database
	client *mongo.Client
	closed bool
}

// New TestDB instance.
func New() *TestDB {
	bgo.SetTestDB()
	client, err := mongo.Connect(context.Background(), bgo.ClientOptions())
	if err != nil {
		panic(err)
	}
	db := client.Database(bgo.Database())

	return &TestDB{db, client, false}
}

// Close TestDB.
func (db *TestDB) Close() error {
	if db.closed {
		return nil
	}

	db.closed = true
	if err := db.Drop(context.Background()); err != nil {
		return err
	}

	return db.client.Disconnect(context.Background())
}

// Test create a test function that uses of mongodb
func Test(f func(db *TestDB, t *testing.T)) tt.TestFunction {
	var db *TestDB
	return tt.Closer(func() io.Closer {
		db = New()
		return db
	}, func(t *testing.T) {
		f(db, t)
	})
}
