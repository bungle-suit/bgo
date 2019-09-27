package ensure

import (
	"context"
	"log"
	"time"

	"github.com/bungle-suit/bgo"

	"github.com/redforks/life"
	"github.com/redforks/testing/reset"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

// Func is a callback to ensure database feature enabled, such as index,
// capped collection etc.
type Func func(ctx context.Context, db *mongo.Database) error

type ensureRec struct {
	fn        Func
	pkg, name string
}

var (
	ensureFuncs []ensureRec
)

// RegisterEnsure register an ensure function to be part of database ensure
// process. Database ensure process is started by "mongo" life cycle package.
//
// Do not assume ensure functions runs at any specific order. Normally, for
// mongodb order is not important.
//
// pkg and name generate log message, do not need to be unique.
func RegisterEnsure(pkg, name string, fn Func) {
	if fn == nil {
		log.Panicf("[%s] Ensure function %s.%s can not be nil", tag, pkg, name)
	}

	life.EnsureStatef(life.Initing, "[%s] Must register ensure function %s.%s in life Initingphase", tag, pkg, name)
	ensureFuncs = append(ensureFuncs, ensureRec{fn, pkg, name})
}

func playEnsure(ctx context.Context, db *mongo.Database) error {
	for _, rec := range ensureFuncs {
		t := time.Now()
		log.Printf("[%s] Ensure: %s (%s)", tag, rec.name, rec.pkg)
		if err := rec.fn(ctx, db); err != nil {
			return err
		}
		log.Printf("[%s] %s (%s), done in %s", tag, rec.name, rec.pkg, time.Since(t))
	}
	return nil
}

func start() {
	if len(ensureFuncs) == 0 {
		return
	}

	ctx := context.Background()
	opts := options.MergeClientOptions(bgo.ClientOptions(),
		options.Client().SetWriteConcern(
			writeconcern.New(writeconcern.WMajority(), writeconcern.J(true))))
	client, err := mongo.Connect(ctx, opts)
	client.StartSession()
	if err != nil {
		log.Panicf("[%s] %w", tag, err)
	}
	defer client.Disconnect(context.Background())

	if err = playEnsure(ctx, client.Database(bgo.Database())); err != nil {
		log.Panicf("[%s] %s", tag, err)
	}
}

func init() {
	life.Register(tag, start, nil)
	reset.Register(func() {
		ensureFuncs = nil
	}, nil)
}
