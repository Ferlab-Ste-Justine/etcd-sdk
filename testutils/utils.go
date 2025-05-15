package testutils

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
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

type TeardownTestCluster func() []error

type EtcdTestMember struct {
	Name       string
	Ip         string
	DataDir    string
	LogFile    string
	ClientPort int64
	PeerPort   int64
}

type EtcdTestClusterOpts struct {
	CaCertPath     string
	ServerCertPath string
	ServerKeyPath  string
	Ips            []string
	ClientPort     int64
	PeerPort       int64
}

func (opts *EtcdTestClusterOpts) SetDefaults(testDir string) {
	if opts.CaCertPath == "" {
		opts.CaCertPath = path.Join(testDir, "certs", "ca.crt")
	}

	if opts.ServerCertPath == "" {
		opts.ServerCertPath = path.Join(testDir, "certs", "server.crt")
	}

	if opts.ServerKeyPath == "" {
		opts.ServerKeyPath = path.Join(testDir, "certs", "server.key")
	}

	if len(opts.Ips) != 3 {
		opts.Ips = []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"}
	}

	if opts.ClientPort == 0 {
		opts.ClientPort = 3379
	}

	if opts.PeerPort == 0 {
		opts.PeerPort = 3380
	}
}

/*
Launch of trio of etcd nodes, running on loopbackaddresses by default and configured for mTLS.
A teardown method is returned to shut them down.
It is assumed that a recent etcd binary is located in the binary path.
*/
func LaunchTestEtcdCluster(testDir string, opts EtcdTestClusterOpts) (TeardownTestCluster, error) {
	opts.SetDefaults(testDir)
	members := []EtcdTestMember{
		EtcdTestMember{
			Name: "etcd0",
			Ip:   opts.Ips[0],
			DataDir: path.Join(testDir, "etcd0-data"),
			LogFile: path.Join(testDir, "etcd-logs", "etcd0.log"),
			ClientPort: opts.ClientPort,
			PeerPort: opts.PeerPort,
		},
		EtcdTestMember{
			Name: "etcd1",
			Ip:   opts.Ips[1],
			DataDir: path.Join(testDir, "etcd1-data"),
			LogFile: path.Join(testDir, "etcd-logs", "etcd1.log"),
			ClientPort: opts.ClientPort,
			PeerPort: opts.PeerPort,
		},
		EtcdTestMember{
			Name: "etcd2",
			Ip:   opts.Ips[2],
			DataDir: path.Join(testDir, "etcd2-data"),
			LogFile: path.Join(testDir, "etcd-logs", "etcd2.log"),
			ClientPort: opts.ClientPort,
			PeerPort: opts.PeerPort,
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
			"--trusted-ca-file", opts.CaCertPath,
			"--cert-file", opts.ServerCertPath,
			"--key-file", opts.ServerKeyPath,
			"--peer-client-cert-auth",
			"--peer-trusted-ca-file", opts.CaCertPath,
			"--peer-cert-file", opts.ServerCertPath,
			"--peer-key-file", opts.ServerKeyPath,
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