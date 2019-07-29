package main

import (
	"github.com/pkg/xattr"
	"os"
)

func main() {
	f, _ := os.OpenFile("/mnt/2/test", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	err := xattr.FSet(f, "user.s3sync.test", []byte("asddad"))
	if err != nil {
		panic(err)
	}

	_, err = xattr.FGet(f, "user.s3sync.test2")
	if err != nil {
		panic(err)
	}
}
