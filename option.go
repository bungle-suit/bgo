package bgo

import (
	"context"
	"fmt"
	"log"
	"time"

	version "github.com/hashicorp/go-version"
	"github.com/redforks/config"
	"github.com/redforks/testing/reset"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type option struct {
	DbURL string

	// Require the mongodb at least be this version, such as "2.6.8", or
	// "2.6", or "2", empty means any version.
	RequiredVersion string

	// Database name of current application work with
	Database string
}

func (o *option) Init() error {
	clientOptions = options.Client().ApplyURI(o.DbURL).
		SetConnectTimeout(2 * time.Second).
		SetSocketTimeout(2 * time.Second)

	database = o.Database

	if err := clientOptions.Validate(); err != nil {
		return err
	}

	ctx := context.Background()
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return err
	}
	defer client.Disconnect(ctx)

	if o.RequiredVersion != "" {
		return o.requireVersion(ctx, client)
	}

	return nil
}

func (o *option) requireVersion(ctx context.Context, client *mongo.Client) error {
	reqVer, err := version.NewVersion(o.RequiredVersion)
	if err != nil {
		return err
	}

	db := client.Database("admin")
	rv := db.RunCommand(ctx, bson.M{"buildInfo": 1})
	doc := bson.M{}
	if err := rv.Decode(&doc); err != nil {
		return err
	}

	dbVer, err := version.NewVersion(doc["version"].(string))
	if err != nil {
		return err
	}
	if dbVer.LessThan(reqVer) {
		return fmt.Errorf("[%s] Required version: %s, got %s", tag, o.RequiredVersion, dbVer.String())
	}
	return nil
}

func (o *option) Apply() {
	log.Printf("[%s] not support apply option, must restart to take effect!", tag)
}

func newDefaultOption() config.Option {
	opt := option{}
	if reset.TestMode() {
		opt.DbURL = testDbURL
		opt.Database = TestDBName
	}
	return &opt
}

func init() {
	config.Register(tag, newDefaultOption)
}
