package reset

import (
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
)

var (
	testModeOnce = sync.Once{}
	_testMode    bool
)

// TestMode returns true if run as unit test
func TestMode() bool {
	testModeOnce.Do(func() {
		_testMode = strings.HasSuffix(os.Args[0], ".test")

		if _testMode {
			log.SetOutput(ioutil.Discard)
		}
	})
	return _testMode
}
