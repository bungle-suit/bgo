// Package hal separate depends on on real hardware and os, such as time, file,
// and other os operations. Use hal package mainly for easier to do unit test.
//
// Nearly all members in this package is unprotected alias of standard go
// functions, do not alter them unless inside unit tests.
package hal

import (
	"os"
	"os/user"
	"time"

	"github.com/redforks/testing/reset"
)

var (
	// Exit is alias of os.Exit
	Exit = os.Exit

	// Now is alias of time.Now
	Now = time.Now

	// Getenv is alias of os.Getenv
	Getenv = os.Getenv

	// CurrentUser is alias of os/user.Current
	CurrentUser = user.Current
)

func init() {
	reset.Register(func() {
		Exit = os.Exit
		Now = time.Now
		Getenv = os.Getenv
		CurrentUser = user.Current
	}, nil)
}
