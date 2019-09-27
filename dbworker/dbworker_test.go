package dbworker_test

import (
	"context"
	"testing"

	"github.com/bungle-suit/bgo"

	"github.com/bungle-suit/bgo/dbworker"
	"github.com/bungle-suit/bgo/testdb"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestDBWorker(t *testing.T) {
	t.Run("empty", testdb.Test(func(db *testdb.TestDB, t *testing.T) {
		worker := dbworker.NewWithDB(db.Database, 3, 5)
		assert.NoError(t, worker.Close())
	}))

	t.Run("with load", testdb.Test(func(db *testdb.TestDB, t *testing.T) {
		worker := dbworker.NewWithDB(db.Database, 3, 5)
		defer func() {
			assert.NoError(t, worker.Close())
		}()

		ctx := context.Background()
		ctx = context.WithValue(ctx, "foo", "bar")
		doc := bson.M{
			"_id": int32(1), "Name": "foo",
		}
		res, err := worker.Do(ctx, func(ctx context.Context, db *mongo.Database) (interface{}, error) {
			assert.Equal(t, "bar", ctx.Value("foo"))
			assert.Equal(t, bgo.TestDBName, db.Name())
			_, err := db.Collection("foo").InsertOne(ctx, doc)
			return 3, err
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, res)

		res, err = worker.Do(ctx, func(ctx context.Context, db *mongo.Database) (interface{}, error) {
			rv := db.Collection("foo").FindOne(ctx, bson.M{"_id": int32(1)})
			var v bson.M
			err := rv.Decode(&v)
			return v, err
		})
		assert.NoError(t, err)
		assert.Equal(t, doc, res)
	}))
}
