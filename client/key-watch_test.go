package client

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestWatchPrefixChanges(t *testing.T) {
	tearDown, launchErr := launchTestEtcdCluster("../test")
	if launchErr != nil {
		t.Errorf("Error occured launching test etcd cluster: %s", launchErr.Error())
		return
	}

	defer func() {
		errs := tearDown()
		if len(errs) > 0 {
			t.Errorf("Errors occured tearing down etcd cluster: %s", errs[0].Error())
		}
	}()

	retryInterval, _ := time.ParseDuration("1s")
	timeouts, _ := time.ParseDuration("10s")
	retries := uint64(10)
	cli := setupTestEnv(t, timeouts, retryInterval, retries)

	prefix := "/inside/"

	getInfo, getErr := cli.GetPrefix(prefix)
	if getErr != nil {
		t.Errorf("Test watch prefix failed. Error occured getting the prefix keys: %s", getErr.Error())
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	go keepChangingLeaderInBackground(t, cli, done, &wg)


	var wgMain sync.WaitGroup
	wgMain.Add(2)
	max := 200
	go func() {
		defer wgMain.Done()

		phases := []string{"insert", "update", "delete"}
		idx := 0
		processedEvents := 0

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		opts := WatchOptions{
			Revision: getInfo.Revision + 1,
			IsPrefix: true,
			TrimPrefix: true,
		}
		watch := cli.SetContext(ctx).Watch(prefix, opts)
		for result := range watch {
			if result.Error != nil {
				t.Errorf("Test watch prefix failed. Error occured while watching a key: %s", result.Error.Error())
			}
			
			changes := result.Changes
			expectedDeletions := 0
			expectedUpserts := 0

			key := fmt.Sprintf("A%d", idx)
			if phases[0] == "insert" {
				expectedUpserts = 1
			} else if phases[0] == "update" {
				expectedUpserts = 1
			} else {
				expectedDeletions = 1
			}

			if len(changes.Deletions) != expectedDeletions || len(changes.Upserts) != expectedUpserts {
				t.Errorf("Test watch prefix failed. During phase %s, expected %d deletions and %d upserts and there were %d deletions and %d upserts", phases[0], expectedDeletions, expectedUpserts, len(changes.Deletions), len(changes.Upserts))
			}

			if phases[0] == "insert" {
				keyInfo, ok := changes.Upserts[key]
				if !ok {
					t.Errorf("Expected key %s to be present in changes during insert phase and it wasn't", key)
				}
				if keyInfo.Value != key {
					t.Errorf("Expected key %s to have value %s in changes during insert phase and it had value %s", key, key, keyInfo.Value)
				}
			} else if phases[0] == "update" {
				keyInfo, ok := changes.Upserts[key]
				if !ok {
					t.Errorf("Expected key %s to be present in changes during update phase and it wasn't", key)
				}
				val := fmt.Sprintf("B%d", idx)
				if keyInfo.Value != val {
					t.Errorf("Expected key %s to have value %s in changes during insert phase and it had value %s", key, val, keyInfo.Value)
				}
			} else {
				if changes.Deletions[0] != key {
					t.Errorf("Expected key %s to be present in changes during deletion phase and it wasn't", key)	
				}
			}

			processedEvents += 1
			idx += 1
			if idx == max {
				idx = 0
				phases = phases[1:]
				if len(phases) == 0 {
					cancel()
				}
			}
		}

		if processedEvents != 600 {
			t.Errorf("Expected watch to process 600 events and it processed %d", processedEvents)
		}
	}()

	go func() {
		defer wgMain.Done()
		for i:=0; i < max; i++ {
			elem := fmt.Sprintf("A%d", i)
			_, putErr := cli.PutKey(prefix + elem, elem)
			if putErr != nil {
				t.Errorf("Test watch prefix failed. Error occured setting a key: %s", putErr.Error())
			}
		}

		for i:=0; i < max; i++ {
			elem := fmt.Sprintf("A%d", i)
			change := fmt.Sprintf("B%d", i)
			_, putErr := cli.PutKey(prefix + elem, change)
			if putErr != nil {
				t.Errorf("Test watch prefix failed. Error occured setting a key: %s", putErr.Error())
			}
		}

		for i:=0; i < max; i++ {
			elem := fmt.Sprintf("A%d", i)
			delErr := cli.DeleteKey(prefix + elem)
			if delErr != nil {
				t.Errorf("Test watch prefix failed. Error occured deleting a key: %s", delErr.Error())
			}
		}
	}()

	wgMain.Wait()

	close(done)
	wg.Wait()
}