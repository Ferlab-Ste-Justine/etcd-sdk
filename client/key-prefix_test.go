package client

import (
	"sync"
	"testing"
	"time"
)

func TestGetPrefix(t *testing.T) {
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

	duration, _ := time.ParseDuration("5s")
	retries := uint64(10)
	cli := setupTestEnv(t, duration, retries)
	
	prefix := "/inside/"
	prefixContent := map[string]string{
		"": "hyf983ghbc9o",
		"dfhd23c=-q0w3kjiur21kjs": "weu2[].dwkeyh",
		"sfhjsax2djh2908h987sfuoeff": "shjsa,[;p1o2kjghfiuash",
		"sjfhlks19087301ahfu82": "syr93ig;lkcphr821",
	}

	for key, val := range prefixContent {
		putErr := cli.PutKey(prefix+ key, val)
		if putErr != nil {
			t.Errorf("Get Prefix test failed. Put test setup returned and error: %s.", putErr.Error())
		}
	}

	notPrefix := "/not-inside/"
	prefixNotContent := map[string]string{
		"": "hyf983ghbc9o",
		"dfhd23g40jlc=-q0w3kjiur21kjs": "weu2[].2mjolhdwkeyh",
		"sfhjsax2gfdghjodjh2908h987sfuoeff": "shjs2j39ja,[;p1o2kjghfiuash",
		"sjfhlks1vhkdsgh9087301ahfu82": "syr93ig;le21j90-kcphr821",
	}

	for key, val := range prefixNotContent {
		putErr := cli.PutKey(notPrefix + key, val)
		if putErr != nil {
			t.Errorf("Get Prefix test failed. Put test setup returned and error: %s.", putErr.Error())
		}
	}
	
	done := make(chan struct{})
	var wg sync.WaitGroup
	go keepChangingLeaderInBackground(t, cli, done, &wg)

	for i:=0; i < 300; i++ {
		info, infoErr := cli.GetPrefix(prefix)
		if infoErr != nil {
			t.Errorf("Get Prefix test failed. Getting prefix returned error: %s.", infoErr.Error())
		}

		if len(info.Keys) != len(prefixContent) {
			t.Errorf("Get Prefix test failed. Getting prefix returned number of results different from number of keys in prefix")
		}

		for key, val := range prefixContent {
			infoVal, ok := info.Keys[prefix + key]
			if !ok {
				t.Errorf("Get Prefix test failed. One of the keys in the prefix was not returned")
			} else if infoVal.Value != val {
				t.Errorf("Get Prefix test failed. One of the keys in the prefix has value mismatch with the result")
			}
		}
	}

	close(done)
	wg.Wait()
}

func TestDeletePrefix(t *testing.T) {
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

	duration, _ := time.ParseDuration("5s")
	retries := uint64(10)
	cli := setupTestEnv(t, duration, retries)
	
	prefix := "/inside/"
	prefixContent := map[string]string{
		"": "hyf983ghbc9o",
		"dfhd23c=-q0w3kjiur21kjs": "weu2[].dwkeyh",
		"sfhjsax2djh2908h987sfuoeff": "shjsa,[;p1o2kjghfiuash",
		"sjfhlks19087301ahfu82": "syr93ig;lkcphr821",
	}

	for key, val := range prefixContent {
		putErr := cli.PutKey(prefix+ key, val)
		if putErr != nil {
			t.Errorf("Delete Prefix test failed. Put test setup returned and error: %s.", putErr.Error())
		}
	}

	notPrefix := "/not-inside/"
	prefixNotContent := map[string]string{
		"": "hyf983ghbc9o",
		"dfhd23g40jlc=-q0w3kjiur21kjs": "weu2[].2mjolhdwkeyh",
		"sfhjsax2gfdghjodjh2908h987sfuoeff": "shjs2j39ja,[;p1o2kjghfiuash",
		"sjfhlks1vhkdsgh9087301ahfu82": "syr93ig;le21j90-kcphr821",
	}

	for key, val := range prefixNotContent {
		putErr := cli.PutKey(notPrefix + key, val)
		if putErr != nil {
			t.Errorf("Delete Prefix test failed. Put test setup returned and error: %s.", putErr.Error())
		}
	}

	delErr := cli.DeletePrefix(prefix)
	if delErr != nil {
		t.Errorf("Delete Prefix test failed. Deleting prefix returned error: %s.", delErr.Error())
	}

	info, infoErr := cli.GetPrefix(prefix)
	if infoErr != nil {
		t.Errorf("Delete Prefix test failed. Getting prefix returned error: %s.", infoErr.Error())
	}

	if len(info.Keys) != 0 {
		t.Errorf("Delete Prefix test failed. Expected 0 keys to be left in prefix after deleting it and there were: %d.", len(info.Keys))
	}

	info, infoErr = cli.GetPrefix(notPrefix)
	if len(info.Keys) != len(prefixNotContent) {
		t.Errorf("Delete Prefix test failed. Expected the number of keys to be the same outside prefix and they weren't.")
	}
}

func TestDiffPrefixWithMap(t *testing.T) {
	//TODO
}

func TestApplyDiffToPrefix(t *testing.T) {
	//TODO
}

func TestDiffBetweenPrefixes(t *testing.T) {
	//TODO
}

func TestWatchPrefixChanges(t *testing.T) {
	//TODO
}