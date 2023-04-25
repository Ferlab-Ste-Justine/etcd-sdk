package client

/*
Differential between two key spaces
*/
type KeyDiff struct {
	//List of keys to insert with the insert value to make it like the source
	Inserts   map[string]string
	//List of keys to update with the update value to make it like the source
	Updates   map[string]string
	//List of keys to delete in the target to make it like the source
	Deletions []string
}

/*
Returns true if a KeyDiff Structure indicates not modifications to the destination
*/
func (diff *KeyDiff) IsEmpty() bool {
	return len(diff.Inserts) == 0 && len(diff.Updates) == 0 && len(diff.Deletions) == 0
}

type KeyDiffFilter func(key string) bool

/*
Filter the keys of a KeyDiff structure by applying a function on each key and only keeping the keys for which the function returns true.
The result of the filter is returned into a separate structure.
*/
func (diff *KeyDiff) FilterKeys(fn KeyDiffFilter) *KeyDiff {
	copy := KeyDiff{
		Inserts: map[string]string{},
		Updates: map[string]string{},
		Deletions: []string{},
	}

	for key, val := range diff.Inserts {
		if fn(key) {
			copy.Inserts[key] = val
		}
	}

	for key, val := range diff.Updates {
		if fn(key) {
			copy.Updates[key] = val
		}
	}

	for _, key :=  range diff.Deletions {
		if fn(key) {
			copy.Deletions = append(copy.Deletions, key)
		}
	}

	return &copy
}

type KeyDiffTransform func(key string) string

/*
Change the keys of a KeyDiff structure by applying a function on each key.
The result of the filter is returned into a separate structure.
*/
func (diff *KeyDiff) TranformKeys(fn KeyDiffTransform) *KeyDiff {
	copy := KeyDiff{
		Inserts: map[string]string{},
		Updates: map[string]string{},
		Deletions: []string{},
	}

	for key, val := range diff.Inserts {
		copy.Inserts[fn(key)] = val
	}

	for key, val := range diff.Updates {
		copy.Updates[fn(key)] = val
	}

	for _, key :=  range diff.Deletions {
		copy.Deletions = append(copy.Deletions, fn(key))
	}

	return &copy
}

/*
Given a desired source keyspace and a destination keyspace that should be modified to be like the source,
it returns the modifications to do on the destination to make it so.
*/
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
