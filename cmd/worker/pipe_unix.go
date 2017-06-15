// +build linux darwin
package main

import (
	"io"
	"os"
	"syscall"
)

func mkPipe(p string) (io.ReadWriteCloser, error) {
	err := syscall.Mkfifo(p, 0600)
	if err != nil {
		return nil, err
	}

	//pipe, err := os.OpenFile(p, os.O_RDWR, 0600)
	//if err != nil {
	//return
	//}
	return os.OpenFile(p, os.O_RDWR, 0600)
}
