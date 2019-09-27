package ensure_test

import (
	"context"
	"testing"

	"github.com/bungle-suit/bgo/ensure"
	"github.com/bungle-suit/bgo/testdb"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestEnsureField(t *testing.T) {
	ctx := context.Background()

	t.Run("Collection not exist", testdb.Test(func(db *testdb.TestDB, t *testing.T) {
		assert.NoError(t, ensure.Field(context.Background(), db.Collection("foo"), "foo", 0))
	}))

	t.Run("Collection exist", testdb.Test(func(db *testdb.TestDB, t *testing.T) {
		coll := db.Collection("foo")
		_, err := coll.InsertMany(ctx, []interface{}{
			bson.M{"_id": 3, "C": 5},
			bson.M{"_id": 4},
			bson.M{"_id": 5, "foo": "exist"},
		})
		assert.NoError(t, err)

		assert.NoError(t, ensure.Field(ctx, db.Collection("foo"), "foo", 0))

		cur, err := coll.Find(ctx, bson.M{}, options.Find().SetProjection(bson.M{}))
		assert.NoError(t, err)
		var rv []bson.M
		assert.NoError(t, cur.All(ctx, &rv))
		assert.Equal(t, []bson.M{
			{"_id": int32(3), "C": int32(5), "foo": int32(0)},
			{"_id": int32(4), "foo": int32(0)},
			{"_id": int32(5), "foo": "exist"},
		}, rv)
	}))
}
