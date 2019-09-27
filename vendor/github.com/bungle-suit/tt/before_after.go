package tt

import (
	"io"
	"testing"
)

// TestFunction function to run by testing.T.Run()
type TestFunction = func(t *testing.T)

// Middleware is a filter/wrap function add common behaviors to test function.
type Middleware = func(f TestFunction) TestFunction

// Before wraps a testing.Run() function that runs before function
// before test.
func Before(before func(), f TestFunction) TestFunction {
	return func(t *testing.T) {
		before()
		f(t)
	}
}

// BeforeFP returns a function that wraps
func BeforeFP(before func()) Middleware {
	return func(f TestFunction) TestFunction {
		return Before(before, f)
	}
}

// After run after function after test function, even if panic
func After(after func(), f TestFunction) TestFunction {
	return func(t *testing.T) {
		defer func() {
			after()

			e := recover()
			if e != nil {
				panic(e)
			}
		}()

		f(t)
	}
}

// AfterFP FP version of After.
func AfterFP(after func()) Middleware {
	return func(f TestFunction) TestFunction {
		return After(after, f)
	}
}

// BeforeAfter run before after function before/after test function,
// after function called even test function paniced.
func BeforeAfter(before func(), after func(), f TestFunction) TestFunction {
	return Before(before, After(after, f))
}

// BeforeAfterFP FP version of BeforeAfter
func BeforeAfterFP(before func(), after func()) Middleware {
	return func(f TestFunction) TestFunction {
		return BeforeAfter(before, after, f)
	}
}

// Closer use Closer interface to do BeforeAfter.
//
// Put before operation in closable function, put after operation in returned
// Closer.Close() method.
func Closer(fClosable func() io.Closer, f TestFunction) TestFunction {
	var closable io.Closer
	return BeforeAfter(func() {
		closable = fClosable()
	}, func() {
		if err := closable.Close(); err != nil {
			panic(err)
		}
	}, f)
}

// CloserFP FP version of Closer().
func CloserFP(fClosable func() io.Closer) Middleware {
	return func(f TestFunction) TestFunction {
		return Closer(fClosable, f)
	}
}
