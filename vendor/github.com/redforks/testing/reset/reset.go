// reset package manage resettable operations useful for unit testing.
// Call reset.setEnabled(true) at begin of the test, call
// reset.setEnabled(false) at the end test.
//
// All initialization code should register a reset function by reset.Add(),
// they will execute on .setEnabled(false). reset.Add() has no effect if reset
// module not enabled.
package reset

import (
	"sync"
	"sync/atomic"
)

type stateType int32

const (
	st_disabled stateType = iota
	st_enabled
	st_enabled_executing
)

var (
	actionQueue []func()
	state       stateType
	mutex       sync.Mutex
)

// Add a reset operation, not add if not enabled.
func Add(action func()) {
	st := stateType(atomic.LoadInt32((*int32)(&state)))
	switch st {
	case st_disabled:
		return
	case st_enabled_executing:
		panic("AddReset during executing reset actions")
	}

	mutex.Lock()
	defer mutex.Unlock()

	actionQueue = append(actionQueue, action)
}

// Enable reset manager.
func Enable() {
	if !atomic.CompareAndSwapInt32((*int32)(&state), int32(st_disabled), int32(st_enabled)) {
		panic("testing/reset already enabled or last test not reset successfully")
	}
}

// Disable reset manager.
// If disabled Add function has no effect.
func Disable() {
	if !atomic.CompareAndSwapInt32((*int32)(&state), int32(st_enabled), int32(st_enabled_executing)) {
		panic("testing/reset already disabled")
	}

	mutex.Lock()
	defer func() {
		// Mark disabled after running reset functions to prevent next test run
		// under unrest environment.
		atomic.StoreInt32((*int32)(&state), int32(st_disabled))
		mutex.Unlock()
	}()

	execResets()
	execResetRecovers()
}

func execResets() {
	q := actionQueue
	actionQueue = nil

	for i := len(q) - 1; i >= 0; i-- {
		q[i]()
	}
}

// Return enabled state. Consider reset.Enabled() as test mode, if it is true,
// caller can sure she is running inside a unit test.
func Enabled() bool {
	return atomic.LoadInt32((*int32)(&state)) != int32(st_disabled)
}
