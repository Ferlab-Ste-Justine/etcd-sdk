package client

import (
	"testing"
	"time"
)

func TestGetMembers(t *testing.T) {
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

	members, membersErr := cli.GetMembers(true)
	if membersErr != nil {
		t.Errorf("Getting members failed: %s", membersErr.Error())
	}

	if len(members.Members) != 3 {
		t.Errorf("Expected 3 members in the reply, not %d", len(members.Members))
	}

	leaders := 0
	for _, member := range members.Members {
		if member.Name != "etcd0" && member.Name != "etcd1" && member.Name != "etcd2" {
			t.Errorf("Name of one of the members had unexpected value of %s", member.Name)
		}

		if len(member.PeerUrls) != 1 {
			t.Errorf("Expected member %s to have 1 peer url, but it had %d", member.Name, len(member.PeerUrls))
		}

		if member.PeerUrls[0] != "https://127.0.0.1:3380" && member.PeerUrls[0] != "https://127.0.0.2:3380" && member.PeerUrls[0] != "https://127.0.0.3:3380" {
			t.Errorf("Member %s has an unexpected peer url of %s", member.Name, member.PeerUrls[0])
		}

		if len(member.ClientUrls) != 1 {
			t.Errorf("Expected member %s to have 1 client url, but it had %d", member.Name, len(member.ClientUrls))
		}

		if member.ClientUrls[0] != "https://127.0.0.1:3379" && member.ClientUrls[0] != "https://127.0.0.2:3379" && member.ClientUrls[0] != "https://127.0.0.3:3379" {
			t.Errorf("Member %s has an unexpected client url of %s", member.Name, member.ClientUrls[0])
		}

		if member.IsLearner {
			t.Errorf("Member %s is marked as a learner which is unexpected", member.Name)
		}

		if member.Status == nil {
			t.Errorf("Member %s has an unset status which is unexpected", member.Name)
		}

		if !member.Status.IsResponsive || member.Status.ResponseError != nil {
			t.Errorf("Member %s did not respond successfully to status request which is unexpected", member.Name)
		}

		if member.Status.ProtocolVersion == "" {
			t.Errorf("Member %s has no protocol version set in its status which is unexpected", member.Name)
		}

		if member.Status.DbSize == int64(0) {
			t.Errorf("Member %s has unset DbSize which is unexpected", member.Name)
		}

		if member.Status.DbSizeInUse == int64(0) {
			t.Errorf("Member %s has unset DbSizeInUse which is unexpected", member.Name)
		}

		if member.Status.IsLeader {
			leaders += 1
		}
	}

	if leaders != 1 {
		t.Errorf("Expected 1 leader to be marked in the status replies, but there were %d", leaders)
	}
}

func TestSetLeaderStatus(t *testing.T) {
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

	testSetLeader := func(leaderName string) {
		err := cli.SetLeaderStatus(leaderName, true)
		if err != nil {
			t.Errorf("Setting %s as leader failed: %s", leaderName, err.Error())
		}
	
		members, membersErr := cli.GetMembers(true)
		if membersErr != nil {
			t.Errorf("Getting members failed: %s", membersErr.Error())
		}
	
		leaders := 0
		for _, member := range members.Members {
			if member.Status.IsLeader {
				leaders += 1
				if member.Name != leaderName {
					t.Errorf("Expected %s to be leader after explicitly specifying it, but instead it was %s", leaderName, member.Name)
				}
			}
		}
	
		if leaders != 1 {
			t.Errorf("Expected 1 leader to be marked in the status replies after specifying a leader, but there were %d", leaders)
		}
	}

	for _, leaderName := range []string{"etcd0", "etcd1", "etcd2", "etcd2"} {
		testSetLeader(leaderName)
	}

	testSetNotLeader := func(notLeaderName string) {
		err := cli.SetLeaderStatus(notLeaderName, false)
		if err != nil {
			t.Errorf("Setting %s as not a leader failed: %s", notLeaderName, err.Error())
		}
	
		members, membersErr := cli.GetMembers(true)
		if membersErr != nil {
			t.Errorf("Getting members failed: %s", membersErr.Error())
		}
	
		leaders := 0
		for _, member := range members.Members {
			if member.Status.IsLeader {
				leaders += 1
				if member.Name == notLeaderName {
					t.Errorf("Expected %s to not be a leader after explicitly specifying it, but it still was", notLeaderName)
				}
			}
		}
	
		if leaders != 1 {
			t.Errorf("Expected 1 leader to be marked in the status replies after specifying a node not to be leader, but there were %d", leaders)
		}
	}

	for _, notLeaderName := range []string{"etcd2", "etcd2", "etcd1", "etcd1", "etcd0", "etcd0"} {
		testSetNotLeader(notLeaderName)
	}
}

func TestChangeLeader(t *testing.T) {
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

	testChangeLeader := func() {
		leaderId := uint64(0)
		leaderFound := false

		members, membersErr := cli.GetMembers(true)
		if membersErr != nil {
			t.Errorf("Getting members failed: %s", membersErr.Error())
		}
	
		for _, member := range members.Members {
			if member.Status.IsLeader {
				leaderId = member.Id
				leaderFound = true
			}

			if !member.Status.IsResponsive {
				t.Errorf("Tried to change leader and found a member in the cluster was unresponsive")
			}
		}

		if !leaderFound {
			t.Errorf("Tried to change leader, but there were none")
		}

		err := cli.ChangeLeader()
		if err != nil {
			t.Errorf("Tried to change leader and encountered the following error: %s", err.Error())
		}

		leaderFound = false
		members, membersErr = cli.GetMembers(true)
		if membersErr != nil {
			t.Errorf("Getting members failed: %s", membersErr.Error())
		}

		for _, member := range members.Members {
			if member.Status.IsLeader {
				if member.Id == leaderId {
					t.Errorf("Expected changing leader to change the leader, but it was still the same")
				}
				leaderFound = true
			}

			if !member.Status.IsResponsive {
				t.Errorf("Tried to change leader and found a member in the cluster was unresponsive")
			}
		}

		if !leaderFound {
			t.Errorf("Expected a leader to be present after changing the leader, but there was none")
		}
	}

	for i:=0; i < 20; i++ {
		testChangeLeader()
	}
}