package client

type KeyDiff struct {
	Inserts   map[string]string
	Updates   map[string]string
	Deletions []string
}

func (diff *KeyDiff) IsEmpty() bool {
	return len(diff.Inserts) == 0 && len(diff.Updates) == 0 && len(diff.Deletions) == 0
}

func GetKeyDiff(src map[string]string, dst map[string]string) KeyDiff {
	diffs := KeyDiff{
		Inserts:   make(map[string]string),
		Updates:   make(map[string]string),
		Deletions: []string{},
	}

	for key, _ := range dst {
		if _, ok := src[key]; !ok {
			diffs.Deletions = append(diffs.Deletions, key)
		}
	}

	for key, srcVal := range src {
		dstVal, ok := dst[key]
		if !ok {
			diffs.Inserts[key] = srcVal
		} else if dstVal != srcVal {
			diffs.Updates[key] = srcVal
		}
	}

	return diffs
}
