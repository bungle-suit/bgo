package osutil

import (
	"io/ioutil"
	"os"
	"path/filepath"
)

// WriteFile write data to a file named filename, exist content will truncate.
// If directory and/or file not exist, create using dirPerm and filePerm.
func WriteFile(filename string, data []byte, dirPerm, filePerm os.FileMode) error {
	err := ioutil.WriteFile(filename, data, filePerm)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}

		if err = os.MkdirAll(filepath.Dir(filename), dirPerm); err != nil {
			return err
		}

		return ioutil.WriteFile(filename, data, filePerm)
	}
	return nil
}
