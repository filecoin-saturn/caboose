package caboose

import (
	"io"
	"time"
)

type TrackingReader struct {
	io.Reader
	firstByte time.Time
	len       int
}

func (tr *TrackingReader) Read(buf []byte) (int, error) {
	n, err := tr.Reader.Read(buf)
	if n > 0 && tr.firstByte.IsZero() {
		tr.firstByte = time.Now()
	}
	tr.len += n
	return n, err
}
