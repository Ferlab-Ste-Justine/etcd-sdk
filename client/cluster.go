package client

import (
	"context"
	"time"
)

type EtcdMember struct {
	Id         uint64
	Name       string
	PeerUrls   []string
	ClientUrls []string
	IsLearner  bool
	IsLeader   bool
}

type EtcdMembers struct {
	ClusterId   uint64
	ResponderId uint64
	Revision    int64
	RaftTerm    uint64
	Members []EtcdMember
}

func (cli *EtcdClient) getMembersWithRetries(retries uint64) (EtcdMembers, error) {
	ctx, cancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer cancel()

	listResp, listErr := cli.Client.MemberList(ctx)
	if listErr != nil {
		if !shouldRetry(listErr, retries) {
			return EtcdMembers{}, listErr
		}

		time.Sleep(cli.RetryInterval)
		return cli.getMembersWithRetries(retries - 1)
	}

	members := EtcdMembers{
		ClusterId: listResp.Header.ClusterId,
		ResponderId: listResp.Header.MemberId,
		Revision: listResp.Header.Revision,
		RaftTerm: listResp.Header.RaftTerm,
		Members: []EtcdMember{},
	}
	for _, member := range listResp.Members {
		members.Members = append(members.Members, EtcdMember{
			Id: member.ID,
			Name: member.Name,
			PeerUrls: member.PeerURLs,
			ClientUrls: member.ClientURLs,
			IsLearner: member.IsLearner,
		})
	}

	return members, nil
}

/*
Sets the leader status on the node with the given name.
If isLeader is true, the node will be elected leader if it isn't.
If isLeader is false, the leadership will be transfered to another node.
The function will return an error if it would cause a change in leadership to a node that is a learner.
*/
func (cli *EtcdClient) GetMembers(leadershipInfo bool) (EtcdMembers, error) {
	members, membersErr := cli.getMembersWithRetries(cli.Retries)
	if (!leadershipInfo) || (membersErr != nil) {
		return members, membersErr
	}
}

/*
Sets the leader status on the node with the given name.
If isLeader is true, the node will be elected leader if it isn't.
If isLeader is false, the leadership will be transfered to another node.
The function will return an error if it would cause a change in leadership to a node that is a learner.
*/
/*func (cli *EtcdClient) SetLeaderStatus(name string, isLeader bool) error {

}*/

/*
Forces a change of leader in the etcd cluster at random.
*/
/*func (cli *EtcdClient) ChangeLeader() error {

}*/