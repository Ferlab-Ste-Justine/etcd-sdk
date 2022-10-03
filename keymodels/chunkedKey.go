package keymodels

import (
	"io"
)

type ChunkedKeySnapshot struct {
	Info     ChunkedKeyInfo
	Revision int64
}

type ChunkedKeyInfo struct {
	Size        int64
	Count 		int64
	Version     int64	
}

type ChunkedKeyPayload struct {
	Key   string
	Value io.ReadCloser
	Size  int64
}