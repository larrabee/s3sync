// +build freebsd

package fs

import (
	"github.com/pkg/xattr"
	"syscall"
)

func isNoXattrData(err error) bool {
	if xErr, ok := err.(*xattr.Error); ok {
		if xErr.Err == syscall.ENOATTR {
			return true
		}
		return false
	}
	return false
}

func isXattrSupported() bool {
	return true
}
