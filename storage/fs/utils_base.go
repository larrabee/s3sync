// +build linux darwin netbsd

package fs

import (
	"github.com/pkg/xattr"
	"syscall"
)

func isNoXattrData(err error) bool {
	if xErr, ok := err.(*xattr.Error); ok {
		return xErr.Err == syscall.ENODATA
	}
	return false
}

func isXattrSupported() bool {
	return true
}
