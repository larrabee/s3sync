// +build !linux,!darwin,!netbsd,!freebsd

package fs

func isNoXattrData(err error) bool {
	return false
}

func isXattrSupported() bool {
	return false
}
