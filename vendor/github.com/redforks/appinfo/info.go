package appinfo

import (
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"

	"github.com/redforks/osutil"
	"github.com/redforks/testing/reset"
	"github.com/redforks/xdgdirs"
)

var (
	version, codeName, installID string
)

// CodeName returns application code name.
func CodeName() string {
	return codeName
}

// Version returns application version.
func Version() string {
	return version
}

// InstallID is a random string generated on the first run, later read from
// /var/lib/spork/[CodeName].id
func InstallID() string {
	return installID
}

// SetInfo set code name and version. Use gen_ver script and git post-commit
// hook to auto generate version. Must call SetInfo() during app initialization
// phase, no sync protection to internal variable.
func SetInfo(name, ver string) {
	if !reset.TestMode() && codeName != "" {
		log.Panicf("[%s] Info already set", tag)
	}

	if name == "" {
		log.Panicf("[%s] CodeName can not be empty", tag)
	}

	codeName = name
	version = ver

	idFile := name + ".id"
	if reset.TestMode() {
		rootdir := os.Getenv("_root_dir")
		if rootdir == "" {
			idFile = ""
		} else {
			idFile = filepath.Join(os.Getenv("_root_dir"), "var/lib/spork", idFile)
		}
	} else {
		idFile = filepath.Join(xdgdirs.DataHome(), "spork", idFile)
	}

	content, err := ioutil.ReadFile(idFile)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Panicf("[%s] Error reading install id file \"%s\"", tag, err)
		}

		installId := strconv.FormatUint(uint64(rand.Uint32()), 32)
		if idFile != "" {
			if err := osutil.WriteFile(idFile, []byte(installId), 0700, 0600); err != nil {
				log.Panicf("[%s] Write install id file failed: %s", tag, err)
			}
		}
		installID = installId
		return
	}

	installID = string(content)

	log.Printf("Application %s started, version: %s, install ID: %s", name, ver, installID)
}

func init() {
	reset.Register(nil, func() {
		codeName, version, installID = "", "", ""

		if reset.TestMode() {
			codeName = "test"
		}
	})
}
