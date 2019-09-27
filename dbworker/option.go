package dbworker

import (
	"log"

	"github.com/redforks/config"
)

type option struct {
	// how many dbwork goroutins
	Instances int

	// dbwork queue length for each instance, times instances get actual length.
	QueueLenPerInstance int
}

func (o *option) Init() error {
	qLen = o.Instances * o.QueueLenPerInstance
	workerInstances = o.Instances

	log.Printf("[%s] Initing: %d instances, queue len: %d", tag, o.Instances, qLen)
	return nil
}

func (o *option) Apply() {
	log.Printf("[%s] not support apply option, must restart to take effect!", tag)
}

func newDefaultOption() config.Option {
	return &option{1, 10}
}

func init() {
	config.Register(tag, newDefaultOption)
}
