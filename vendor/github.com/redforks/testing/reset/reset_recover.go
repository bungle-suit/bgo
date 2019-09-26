package reset

var (
	resetFns, recoverFns []func()
)

// ClearInternal used by reset package unit tests only, never call ClearInternal outside reset package
func ClearInternal() {
	resetFns = nil
	recoverFns = nil
}

// Register() register two function to cooperate testing reset/recover process.
// Unlike reset.Add(), which register a reset function to undo things for
// current call, after current test function finished. Register() normally
// register at package init() function to be part of testing cycle. Most
// reset.Add() can be replaced by Register().
//
// Register() is better than Add():
//  1. Better performance, when application runs in non-test mode, do not need
//  over and over calling reset.Add().
//  2. Add() not work well with static inited items, such as ert tree. Ert tree
//  need reset itself to give a clean environment for next test. But unit test
//  also depends on some pre-registered items in the ert tree, simply reset the
//  whole ert tree can not work, fine control which ert entry need reset is too
//  complex. Use Register() each package can re-register static ert items in
//  the recover function.
//
// fReset function called in ginkgo AfterEach() stage (actually in
// reset.Disable()), fReset function called in reversed register order. After
// all fReset() function called, fRecover() functions called by register order.
//
// Both fReset and fRecover can be nil.
//
// fRecover function run immediately on Register to do init job.
func Register(fReset func(), fRecover func()) {
	if TestMode() {
		if fReset != nil {
			resetFns = append(resetFns, fReset)
		}
		if fRecover != nil {
			recoverFns = append(recoverFns, fRecover)
		}
	}

	if fRecover != nil {
		fRecover()
	}
}

func execResetRecovers() {
	for i := len(resetFns) - 1; i >= 0; i-- {
		resetFns[i]()
	}

	for _, f := range recoverFns {
		f()
	}
}
