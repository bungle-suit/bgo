// Helper functions for os related operations.
package osutil

import (
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"syscall"
)

// Set process ulimit of max opened files to max online endpoints + 50,
// if set failed, call os.Exit(-2).
func SetMaxOpenFiles(n uint64) {
	const msg = "Set max open files(sockets) limits to "

	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		logAndExit(-2, msg, n, " failed: ", err)
	}
	rLimit.Max = n
	rLimit.Cur = n
	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		logAndExit(-2, msg, n, " failed: ", err)
	}
	log.Print(msg, n, " okay")
}

// Set OS tcp keep alive parameters.
// idle: How many seconds to seed keep alive package after last send data
// intvl: If not receive keep alive package response, wait how many seconds to
// send another one (prob).
// probes: How many probs tried and not receive response, tcp think the link has
// gone.
// If failed, call os.Exit(-3)
// Currently only support linux.
func SetIpv4TCPKeepAliveParameters(idle, intvl, probes int) {
	const path_prefix = "/proc/sys/net/ipv4/tcp_keepalive_"

	for p, v := range map[string]int{
		"intvl":  intvl,
		"time":   idle,
		"probes": probes,
	} {
		s := strconv.Itoa(v)
		if err := ioutil.WriteFile(path_prefix+p, []byte(s), 0); err != nil {
			logAndExit(-3, "Error set tcp_keepalive_"+p, err)
		} else {
			log.Print("Set tcp_keepalive_", p, " to ", s)
		}
	}
}

func logAndExit(exitCode int, msgs ...interface{}) {
	log.Print(msgs...)
	os.Exit(exitCode)
}
