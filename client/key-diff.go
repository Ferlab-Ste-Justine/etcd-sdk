package client

import "regexp"

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

/*
Filter the content of the KeyDiff structure and keep only the keys that match a given regex.
The result of the filter is returned into a separate structure.
*/
func (diff *KeyDiff) FilterKeys(filter *regexp.Regexp) *KeyDiff {
	copy := KeyDiff{
		Inserts: map[string]string{},
		Updates: map[string]string{},
		Deletions: []string{},
	}

	for key, val := range diff.Inserts {
		if filter.MatchString(key) {
			copy.Inserts[key] = val
		}
	}

	for key, val := range diff.Updates {
		if filter.MatchString(key) {
			copy.Updates[key] = val
		}
	}

	for _, key :=  range diff.Deletions {
		if filter.MatchString(key) {
			copy.Deletions = append(copy.Deletions, key)
		}
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
