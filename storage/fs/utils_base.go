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

type ListErrMask uint8

func (f ListErrMask) Has(flag ListErrMask) bool { return f&flag != 0 }

const (
	ListErrSkipAll ListErrMask = 1 << iota
	ListErrSkipNotExist
	ListErrSkipPermission
)
