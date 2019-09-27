// Package dbworker receiver mongodb access request, and processed by one or more worker goroutines.
// Provide features such as:
//  monitor db load, by monitor db request queue length
//  put slow db operation to specific queue, do not put too much pressure on database
//  better handling database fatal error, such as database shutdown
package dbworker

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/bungle-suit/bgo"
	"github.com/redforks/errors"
	"github.com/redforks/life"
	"go.mongodb.org/mongo-driver/mongo"
)

// Worker maintains a mongodb work queue
type Worker struct {
	reqCh chan *request
	wg    sync.WaitGroup
	db    *mongo.Database

	// true if database is attached using NewWithDB, should close db by NewWithDB() caller.
	attachedDB bool
}

// New create worker, connect to default bgo configured mongodb.
func New(workers int, queueLen int) *Worker {
	client, err := mongo.Connect(context.Background(), bgo.ClientOptions())
	if err != nil {
		panic(err)
	}
	db := client.Database(bgo.Database())
	r := NewWithDB(db, workers, queueLen)
	r.attachedDB = false
	return r
}

// NewWithDB create worker with specific database.
func NewWithDB(db *mongo.Database, workers int, queueLen int) *Worker {
	reqCh := make(chan *request, queueLen)
	r := &Worker{
		db:         db,
		reqCh:      reqCh,
		attachedDB: true,
	}

	r.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go r.run()
	}
	log.Printf("[%s] Started %d goroutines, queue length: %d", tag, workers, queueLen)

	return r
}

// Close closes queue, wait for current work load complete, close database.
func (w *Worker) Close() error {
	close(w.reqCh)

	log.Printf("[%s] Stopping", tag)
	w.wg.Wait()
	if !w.attachedDB {
		client := w.db.Client()
		return client.Disconnect(context.Background())
	}
	return nil
}

func (w *Worker) run() {
	for req := range w.reqCh {
		res, err := req.do(w.db)
		req.ch <- &response{res, err}
	}
	w.wg.Done()
}

// Do put work into queue, wait for its execution and returns result.
func (w *Worker) Do(ctx context.Context, work Work) (interface{}, error) {
	// TODO: cancel mongodb request on ctx.Done().
	req := request{ctx, work, make(chan *response)}
	w.reqCh <- &req
	res := <-req.ch
	return res.res, res.err
}

// Work function do the actual database operation.
type Work func(ctx context.Context, db *mongo.Database) (res interface{}, err error)
type request struct {
	ctx  context.Context
	work Work
	// return result through this channel
	ch chan *response
}

func (r *request) do(db *mongo.Database) (res interface{}, err error) {
	defer func() {
		r := recover()
		if r != nil {
			errors.Handle(context.Background(), r)
			if err == nil {
				var ok bool
				if err, ok = r.(error); !ok {
					err = errors.Bug(fmt.Sprint(r))
				}
			}
		}
	}()

	return r.work(r.ctx, db)
}

type response struct {
	res interface{}
	err error
}

// Do queue a db worker.into default DBWorker.
func Do(ctx context.Context, work Work) (interface{}, error) {
	if defaultWorker == nil {
		log.Panicf("[%s] not inited", tag)
	}

	return defaultWorker.Do(ctx, work)
}

var (
	defaultWorker   *Worker
	workerInstances int
	qLen            int
)

// Start db worker. Normally do not need call Start(), it is auto called in
// life.Start(). It is used in some unit tests.
func start() {
	if defaultWorker != nil {
		log.Panicf("[%s] default worker already exist", tag)
	}

	defaultWorker = New(workerInstances, qLen)
}

func stop() {
	if defaultWorker != nil {
		err := defaultWorker.Close()
		defaultWorker = nil
		if err != nil {
			log.Printf("[%s] %w", tag, err)
		}
	}
	log.Printf("[%s] Stopped", tag)
}

func init() {
	life.Register(tag, start, stop)
}
