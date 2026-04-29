package main

import (
	"io"
	"os"
)

func skipBOM(f *os.File) {
	buf := make([]byte, 3)
	n, err := f.Read(buf)
	if err != nil || n < 3 || buf[0] != 0xEF || buf[1] != 0xBB || buf[2] != 0xBF {
		f.Seek(0, io.SeekStart)
	}
}