package ensure

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// Field is a help ensure function to set field if it dose not exist.
// It is not an error if the collection is not exist.
func Field(ctx context.Context, coll *mongo.Collection, field string, val interface{}) error {
	info, err := coll.UpdateMany(ctx, bson.M{
		field: bson.M{"$exists": false},
	}, bson.M{
		"$set": bson.M{field: val},
	})
	log.Printf(
		"[%s] set %d documents for missing field %s.%s to %v",
		tag, info.ModifiedCount, coll.Name(), field, val)
	return err
}
