package bgo

import (
	"log"

	"github.com/bungle-suit/tt"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	tag = "mongo"

	// TestDBName dbworker default test database name
	TestDBName = "unittest"

	localDbURL = "mongodb://127.0.0.1"
)

var (
	clientOptions *options.ClientOptions
	database      string
)

// ClientOptions return mongo db connection options, panic if the dbUrl not configured
func ClientOptions() *options.ClientOptions {
	if clientOptions == nil {
		log.Panicf("[%s] not configured", tag)
	}
	return clientOptions
}

// Database returns configured mongo database name.
func Database() string {
	if database == "" {
		log.Panicf("[%s] Database not set", tag)
	}
	return database
}

// SetTestDB set ClientOptions to 127.0.0.1/TestDBName
func SetTestDB() {
	if !tt.TestMode() {
		panic("Call SetTestDB only in unit test mode")
	}

	if err := (&option{
		DbURL:    localDbURL,
		Database: TestDBName,
	}).Init(); err != nil {
		panic(err)
	}
}
