package testdb

import (
	"context"

	"github.com/bungle-suit/bgo"
	"go.mongodb.org/mongo-driver/mongo"
)

// TestDb create a connection to mongodb, and delete the database in reset. Also
// provide mongodb operation methods, so we do not need to deal with returned error.
// *mgo.Databae is nested in, so all Database method con be used directly from TestDb, such as:
//
//   db := testdb.New("blah_test")
//   db.C("tbl").Insert(...
//
// Instead of:
//
//   db.Session.DB("").C("tbl").Insert(...
type TestDb struct {
	*mongo.Database
	client *mongo.Client
	closed bool
}

// New TestDb instance.
func New() *TestDb {
	client, err := mongo.Connect(context.Background(), bgo.ClientOptions())
	if err != nil {
		panic(err)
	}
	db := client.Database(bgo.Database())

	return &TestDb{db, client, false}
}

// Close TestDb.
func (db *TestDb) Close() error {
	if db.closed {
		return nil
	}

	db.closed = true
	if err := db.Drop(context.Background()); err != nil {
		return err
	}

	return db.client.Disconnect(context.Background())
}
