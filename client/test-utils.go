package client

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"testing"
	"strings"
	"sync"
	"time"
)

func removeIfExists(path string) (error) {
	_, err := os.Stat(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}

		return nil
	}

	return os.RemoveAll(path)
}


type teardownTestCluster func() []error

type etcdTestMember struct {
	Name       string
	Ip         string
	DataDir    string
	LogFile    string
	ClientPort int64
	PeerPort   int64
}

func launchTestEtcdCluster(testDir string) (teardownTestCluster, error) {
	members := []etcdTestMember{
		etcdTestMember{
			Name: "etcd0",
			Ip:   "127.0.0.1",
			DataDir: path.Join(testDir, "etcd0-data"),
			LogFile: path.Join(testDir, "etcd-logs", "etcd0.log"),
			ClientPort: 3379,
			PeerPort: 3380,
		},
		etcdTestMember{
			Name: "etcd1",
			Ip:   "127.0.0.2",
			DataDir: path.Join(testDir, "etcd1-data"),
			LogFile: path.Join(testDir, "etcd-logs", "etcd1.log"),
			ClientPort: 3379,
			PeerPort: 3380,
		},
		etcdTestMember{
			Name: "etcd2",
			Ip:   "127.0.0.3",
			DataDir: path.Join(testDir, "etcd2-data"),
			LogFile: path.Join(testDir, "etcd-logs", "etcd2.log"),
			ClientPort: 3379,
			PeerPort: 3380,
		},
	}

	err := removeIfExists(path.Join(testDir, "etcd-logs"))
	if err != nil {
		return func() []error {return nil}, err
	}

	for _, member := range members {
		err := removeIfExists(member.DataDir)
		if err != nil {
			return func() []error {return nil}, err
		}
	}

	err = os.MkdirAll(path.Join(testDir, "etcd-logs"), 0770)
	if err != nil {
		return func() []error {return nil}, err
	}

	
	initalClusterArr := []string{}
	for _, member := range members {
		initalClusterArr = append(initalClusterArr, fmt.Sprintf("%s=https://%s:%d", member.Name, member.Ip, member.PeerPort))
	}
	initialCluster := strings.Join(initalClusterArr, ",")


	cmds := []*exec.Cmd{}
	for _, member := range members {
		cmd := exec.Command(
			"etcd", "--name", member.Name,
			"--advertise-client-urls", fmt.Sprintf("https://%s:%d", member.Ip, member.ClientPort),
		    "--listen-client-urls", fmt.Sprintf("https://%s:%d", member.Ip, member.ClientPort),
			"--initial-advertise-peer-urls", fmt.Sprintf("https://%s:%d", member.Ip, member.PeerPort),
			"--listen-peer-urls", fmt.Sprintf("https://%s:%d", member.Ip, member.PeerPort),
			"--initial-cluster-token", "etcd-test-cluster",
			"--initial-cluster", initialCluster,
			"--client-cert-auth",
			"--trusted-ca-file", path.Join(testDir, "certs", "ca.pem"),
			"--cert-file", path.Join(testDir, "certs", "server.pem"),
			"--key-file", path.Join(testDir, "certs", "server.key"),
			"--peer-client-cert-auth",
			"--peer-trusted-ca-file", path.Join(testDir, "certs", "ca.pem"),
			"--peer-cert-file", path.Join(testDir, "certs", "server.pem"),
			"--peer-key-file", path.Join(testDir, "certs", "server.key"),
			"--data-dir", member.DataDir,
			"--log-outputs", member.LogFile,
			"--initial-cluster-state", "new",
		)
		err := cmd.Start()
		if err != nil {
			for _, cmd := range cmds {
				cmd.Process.Kill()
			}
			return func() []error {return nil}, err
		}
		cmds = append(cmds, cmd)
	}

	return func() []error {
		errs := []error{}
		for _, cmd := range cmds {
			err := cmd.Process.Kill()
			if err != nil {
				errs = append(errs, err)
			} else {
				cmd.Process.Wait()
			}
		}
		return errs
	}, nil
}

func keepChangingLeaderInBackground(t *testing.T, cli *EtcdClient, done <-chan struct{}, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	d, _ := time.ParseDuration("1s")

	for true {
		select {
		case <-time.After(d):
			err := cli.ChangeLeader()
			if err != nil {
				t.Errorf("Error occured while changing leader in the background: %s", err.Error())
			}
		case <-done:
			return
		}
	}
}

func setupTestEnv(t *testing.T, timeouts time.Duration, retryInt time.Duration, retries uint64) *EtcdClient {
	cli, err := Connect(context.Background(), EtcdClientOptions{
		ClientCertPath:    "../test/certs/root.pem",
		ClientKeyPath:     "../test/certs/root.key",
		CaCertPath:        "../test/certs/ca.pem",
		EtcdEndpoints:     []string{"127.0.0.1:3379", "127.0.0.2:3379", "127.0.0.3:3379"},
		ConnectionTimeout: timeouts,
		RequestTimeout:    timeouts,
		RetryInterval:     retryInt,
		Retries:           retries,
	})

	if err != nil {
		t.Errorf("Test setup failed at the connection stage: %s", err.Error())
	}

	user := EtcdUser{
		Username: "root",
		Password: "",
		Roles: []string{"root"},
	}

	err = cli.UpsertUser(user)
	if err != nil {
		t.Errorf("Test setup failed at the root user creation stage: %s", err.Error())
	}

	err = cli.SetAuthStatus(true)
	if err != nil {
		t.Errorf("Test setup failed at the auth enabling stage: %s", err.Error())
	}

	return cli
}