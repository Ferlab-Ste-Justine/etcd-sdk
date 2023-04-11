package client

import (
	"testing"
	"time"
)

func TestGetMembers(t *testing.T) {
	duration, _ := time.ParseDuration("5s")
	retries := uint64(10)
	cli := setupTestEnv(t, duration, retries)

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

	teardownTestEnv(t, cli)
}