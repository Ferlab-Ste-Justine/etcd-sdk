package client

import (
	"testing"
)

func TestKeyDiffIsEmpty(t *testing.T) {
	empty := KeyDiff{}
	if !empty.IsEmpty() {
		t.Errorf("Excepted empty key diff to mark as empty and it didn't")
	}

	inserts := KeyDiff{Inserts: map[string]string{"test": "test"}}
	if inserts.IsEmpty() {
		t.Errorf("Excepted key diff with inserts to mark as non-empty and it did")
	}

	updates := KeyDiff{Updates: map[string]string{"test": "test"}}
	if updates.IsEmpty() {
		t.Errorf("Excepted key diff with updates to mark as non-empty and it did")
	}

	deletions := KeyDiff{Deletions: []string{"test"}}
	if deletions.IsEmpty() {
		t.Errorf("Excepted key diff with deletions to mark as non-empty and it did")
	}
}

func TestGetKeyDiff(t *testing.T) {
	empty := map[string]string{}

	noDiff := GetKeyDiff(empty, empty)
	if !noDiff.IsEmpty() {
		t.Errorf("Excepted GetKeyDiff on two empty maps to be empty and it wasn't")
	}

	map1 := map[string]string{
		"test": "test",
		"test2": "test2",
	}

	noDiff = GetKeyDiff(map1, map1)
	if !noDiff.IsEmpty() {
		t.Errorf("Excepted GetKeyDiff on two identical non-empty maps to be empty and it wasn't")
	}

	map2 := map[string]string{
		"test2": "different",
		"test3": "test3",
	}

	diff := GetKeyDiff(map1, map2)

	if len(diff.Deletions) != 1 || diff.Deletions[0] != "test3" {
		t.Errorf("Excepted GetKeyDiff to flag the right removed element and it didn't")
	}

	val, ok := diff.Updates["test2"]
	if (!ok) || len(diff.Updates) != 1 || val != "test2" {
		t.Errorf("Excepted GetKeyDiff to flag the right updated element with the right value and it didn't")
	}

	val, ok = diff.Inserts["test"]
	if (!ok) || len(diff.Inserts) != 1 || val != "test" {
		t.Errorf("Excepted GetKeyDiff to flag the right inserted element with the right value and it didn't")
	}
}