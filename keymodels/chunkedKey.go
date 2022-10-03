package keymodels

import (
	"io"
	"strings"
)

type ChunkedKeySnapshot {
	Info        ChunkedKeyInfo
	Revision int64
}

type ChunkedKeyInfo {
	Size        int64
	Count 		int64
	Version     int64	
}

type ChunkedKeyPayload {
	Key string
	Value io.ReadCloser
	Size int64
}