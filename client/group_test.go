package client

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Ferlab-Ste-Justine/etcd-sdk/testutils"
)

func TestJoinGroup(t *testing.T) {
	tearDown, launchErr := testutils.LaunchTestEtcdCluster("../test", testutils.EtcdTestClusterOpts{})
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

	err := cli.JoinGroup("/groupA/", "Steve", "blue")
	if err != nil {
		t.Errorf("Error occured joining a group: %s", err.Error())
	}
	err = cli.JoinGroup("/groupA/", "Allan", "green")
	if err != nil {
		t.Errorf("Error occured joining a group: %s", err.Error())
	}
	err = cli.JoinGroup("/groupB/", "Edd", "red")
	if err != nil {
		t.Errorf("Error occured joining a group: %s", err.Error())
	}

	members, _, memErr := cli.GetGroupMembers("/groupA/")
	if memErr != nil {
		t.Errorf("Error occured getting members of a group: %s", memErr.Error())
	}

	if len(members) != 2 {
		t.Errorf("Expected 2 members and there were: %d", len(members))
	}

	for _, val := range [][]string{[]string{"Steve", "blue"}, []string{"Allan", "green"}} {
		content, ok := members[val[0]]
		if !ok {
			t.Errorf("Expected member %s to exist and it didn't", val[0])
		}

		if content != val[1] {
			t.Errorf("Expected member %s's content to be %s and it was %s", val[0], val[1], content)
		}
	}

	close(done)
	wg.Wait()
}

func TestLeaveGroup(t *testing.T) {
	tearDown, launchErr := testutils.LaunchTestEtcdCluster("../test", testutils.EtcdTestClusterOpts{})
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

	err := cli.JoinGroup("/groupA/", "Steve", "blue")
	if err != nil {
		t.Errorf("Error occured joining a group: %s", err.Error())
	}
	err = cli.JoinGroup("/groupA/", "Allan", "green")
	if err != nil {
		t.Errorf("Error occured joining a group: %s", err.Error())
	}

	members, _, memErr := cli.GetGroupMembers("/groupA/")
	if memErr != nil {
		t.Errorf("Error occured getting members of a group: %s", memErr.Error())
	}

	if len(members) != 2 {
		t.Errorf("Expected 2 members and there were: %d", len(members))
	}

	err = cli.LeaveGroup("/groupA/", "Allan")
	if err != nil {
		t.Errorf("Error occured leaving a group: %s", err.Error())
	}

	members, _, memErr = cli.GetGroupMembers("/groupA/")
	if memErr != nil {
		t.Errorf("Error occured getting members of a group: %s", memErr.Error())
	}

	if len(members) != 1 {
		t.Errorf("Expected 1 members and there were: %d", len(members))
	}

	content, ok := members["Steve"]
	if !ok {
		t.Errorf("Expected member %s to exist and it didn't", "Steve")
	}

	if content != "blue" {
		t.Errorf("Expected member %s's content to be %s and it was %s", "Steve", "blue", content)
	}

	err = cli.LeaveGroup("/groupA/", "Steve")
	if err != nil {
		t.Errorf("Error occured leaving a group: %s", err.Error())
	}

	members, _, memErr = cli.GetGroupMembers("/groupA/")
	if memErr != nil {
		t.Errorf("Error occured getting members of a group: %s", memErr.Error())
	}

	if len(members) != 0 {
		t.Errorf("Expected 0 members and there were: %d", len(members))
	}

	close(done)
	wg.Wait()
}

func TestWaitGroupCountThreshold(t *testing.T) {
	tearDown, launchErr := testutils.LaunchTestEtcdCluster("../test", testutils.EtcdTestClusterOpts{})
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

	var wgMain sync.WaitGroup
	wgMain.Add(30)

	for idx := 1; idx < 31; idx++ {
		go func(wg *sync.WaitGroup) {
			done := make(chan struct{})
			defer func() {
				close(done)
				wgMain.Done()
			}()
			
			err := cli.JoinGroup("/groupA/", fmt.Sprintf("member%d", idx), fmt.Sprintf("member%d", idx))
			if err != nil {
				t.Errorf("Error occured joining a group: %s", err.Error())
			}

			waitCh := cli.WaitGroupCountThreshold("/groupA/", 30, done)
			err = <-waitCh
			if err != nil {
				t.Errorf("Error occured waiting a group: %s", err.Error())
			}

			members, _, memErr := cli.GetGroupMembers("/groupA/")
			if memErr != nil {
				t.Errorf("Error occured getting members of a group: %s", memErr.Error())
			}
		
			if len(members) != 30 {
				t.Errorf("Expected 30 members and there were: %d", len(members))
			}
		}(&wgMain)
		time.Sleep(200 * time.Millisecond)
	}

	wgMain.Wait()

	close(done)
	wg.Wait()
}