package keymodels

type KeyWatchInfo struct {
	Value          string
	Version        int64
	CreateRevision int64
	ModRevision    int64
	Lease          int64
}

type WatchInfo struct {
	Upserts   map[string]KeyWatchInfo
	Deletions []string
}