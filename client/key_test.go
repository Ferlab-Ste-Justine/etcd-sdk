package client

import (
	"sync"
	"testing"
	"time"
)

func TestGetKey(t *testing.T) {
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

	rev1, err1 := cli.PutKey("test", "testv1")
	if err1 != nil {
		t.Errorf("Error occured setting key value: %s", err1.Error())
	}

	rev2, err2 := cli.PutKey("test", "testv2")
	if err2 != nil {
		t.Errorf("Error occured setting key value: %s", err2.Error())
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	go keepChangingLeaderInBackground(t, cli, done, &wg)

	for i:=0; i < 200; i++ {
		info, err := cli.GetKey("test", GetKeyOptions{Revision: rev1})
		if err != nil {
			t.Errorf("Error occured getting key value: %s", err.Error())
		}
		if !info.Found() {
			t.Errorf("Expected key to be found when getting a key at past revision and it wasn't")	
		}
		if info.Value != "testv1" {
			t.Errorf("Expected testv1 when getting a key at past revision and got: %s", info.Value)
		}

		info, err = cli.GetKey("test", GetKeyOptions{Revision: rev2})
		if err != nil {
			t.Errorf("Error occured getting key value: %s", err.Error())
		}
		if !info.Found() {
			t.Errorf("Expected key to be found when getting a key at later revision and it wasn't")	
		}
		if info.Value != "testv2" {
			t.Errorf("Expected testv2 when getting a key at later revision and got: %s", info.Value)
		}

		info, err = cli.GetKey("test", GetKeyOptions{})
		if err != nil {
			t.Errorf("Error occured getting key value: %s", err.Error())
		}
		if !info.Found() {
			t.Errorf("Expected key to be found when getting latest key and it wasn't")	
		}
		if info.Value != "testv2" {
			t.Errorf("Expected testv2 when getting latest key value and got: %s", info.Value)
		}

		info, err = cli.GetKey("does-not-exist", GetKeyOptions{})
		if err != nil {
			t.Errorf("Error occured getting key value: %s", err.Error())
		}
		if info.Found() {
			t.Errorf("Expected key not to be found when getting non-existent key and it was")	
		}
	}

	close(done)
	wg.Wait()
}

func TestPutKey(t *testing.T) {
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

	done := make(chan struct{})
	var wg sync.WaitGroup
	go keepChangingLeaderInBackground(t, cli, done, &wg)

	for i:=0; i < 200; i++ {
		_, err1 := cli.PutKey("test", "testv1")
		if err1 != nil {
			t.Errorf("Error occured setting key value: %s", err1.Error())
		}
	
		info, err := cli.GetKey("test", GetKeyOptions{})
		if err != nil {
			t.Errorf("Error occured getting key value: %s", err.Error())
		}
		if info.Value != "testv1" {
			t.Errorf("Expected key value to be testv1 after setting it and got: %s", info.Value)
		}

		_, err2 := cli.PutKey("test", "testv2")
		if err2 != nil {
			t.Errorf("Error occured setting key value: %s", err2.Error())
		}

		info, err = cli.GetKey("test", GetKeyOptions{})
		if err != nil {
			t.Errorf("Error occured getting key value: %s", err.Error())
		}
		if info.Value != "testv2" {
			t.Errorf("Expected key value to be testv2 after setting it and got: %s", info.Value)
		}
	}

	close(done)
	wg.Wait()
}

func TestDeleteKey(t *testing.T) {
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

	done := make(chan struct{})
	var wg sync.WaitGroup
	go keepChangingLeaderInBackground(t, cli, done, &wg)

	for i:=0; i < 200; i++ {
		_, err1 := cli.PutKey("test", "testv1")
		if err1 != nil {
			t.Errorf("Error occured setting key value: %s", err1.Error())
		}
	
		info, err := cli.GetKey("test", GetKeyOptions{})
		if err != nil {
			t.Errorf("Error occured getting key value: %s", err.Error())
		}
		if info.Value != "testv1" {
			t.Errorf("Expected key value to be testv1 after setting it and got: %s", info.Value)
		}

		err2 := cli.DeleteKey("test")
		if err2 != nil {
			t.Errorf("Error occured deleting key: %s", err2.Error())
		}

		info, err = cli.GetKey("test", GetKeyOptions{})
		if err != nil {
			t.Errorf("Error occured getting key value: %s", err.Error())
		}
		if info.Found() {
			t.Errorf("Expected key to be missing after deleting it and it was found.")
		}
	}

	close(done)
	wg.Wait()
}