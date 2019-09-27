package testdb_test

import (
	"context"
	"testing"

	"github.com/redforks/errors"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/bungle-suit/bgo"

	"github.com/bungle-suit/bgo/testdb"
)

func assertCollNotExist(t *testing.T, collName string) {
	ctx := context.Background()
	client, err := mongo.Connect(ctx, bgo.ClientOptions())
	if err != nil {
		panic(err)
	}

	db := client.Database(bgo.Database())
	collNames, err := db.ListCollectionNames(ctx, bson.M{})
	assert.NoError(t, err)
	assert.NotContains(t, collNames, "foo")
}

func TestTestDb(t *testing.T) {
	bgo.SetTestDB()
	db := testdb.New()
	defer errors.Close(db)

	assertCollNotExist(t, "foo")
	ctx := context.Background()

	_, err := db.Collection("foo").InsertOne(ctx, bson.M{"_id": 33})
	assert.NoError(t, err)
	assert.NoError(t, db.Close())

	assertCollNotExist(t, "foo")
}

func TestWrap(t *testing.T) {
	t.Run("Test", testdb.Test(func(db *testdb.TestDB, t *testing.T) {
		assertCollNotExist(t, "foo")
		ctx := context.Background()
		_, err := db.Collection("foo").InsertOne(ctx, bson.M{"_id": 33})
		assert.NoError(t, err)
		assert.NoError(t, db.Close())
	}))
	assertCollNotExist(t, "foo")
}
