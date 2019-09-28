// +build linux darwin netbsd

package fs

import (
	"github.com/pkg/xattr"
	"syscall"
)

func isNoXattrData(err error) bool {
	if xErr, ok := err.(*xattr.Error); ok {
		if xErr.Err == syscall.ENODATA {
			return true
		}
		return false
	}
	return false
}

func isXattrSupported() bool {
	return true
}
