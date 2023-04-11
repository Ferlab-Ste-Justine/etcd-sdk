package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type EtcdMemberStatus struct {
	IsLeader         bool
	IsResponsive     bool
	ResponseError    error
	ProtocolVersion  string
	DbSize           int64
	DbSizeInUse      int64
	RaftIndex        uint64
	RaftTerm         uint64
	RaftAppliedIndex uint64
}

type EtcdMember struct {
	Id               uint64
	Name             string
	PeerUrls         []string
	ClientUrls       []string
	IsLearner        bool
	Status           *EtcdMemberStatus
}

type EtcdMembers struct {
	ClusterId    uint64
	RespId       uint64
	RespRevision int64
	RespRaftTerm uint64
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
		ClusterId:    listResp.Header.ClusterId,
		RespId:       listResp.Header.MemberId,
		RespRevision: listResp.Header.Revision,
		RespRaftTerm: listResp.Header.RaftTerm,
		Members:      []EtcdMember{},
	}
	for _, member := range listResp.Members {
		members.Members = append(members.Members, EtcdMember{
			Id: member.ID,
			Name: member.Name,
			PeerUrls: member.PeerURLs,
			ClientUrls: member.ClientURLs,
			IsLearner: member.IsLearner,
			Status: nil,
		})
	}

	return members, nil
}

func (cli *EtcdClient) getEndpointStatusWithRetries(endpoint string, retries uint64) (*clientv3.StatusResponse, error) {
	ctx, cancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer cancel()

	status, statusErr := cli.Client.Status(ctx, endpoint)
	if statusErr != nil {
		if !shouldRetry(statusErr, retries) {
			return nil, statusErr
		}

		time.Sleep(cli.RetryInterval)
		return cli.getEndpointStatusWithRetries(endpoint, retries - 1)
	}

	return status, statusErr
}

/*
Get members info in the cluster.
If statusInfo is true, additional status info will be fetched from each node in the cluster
*/
func (cli *EtcdClient) GetMembers(statusInfo bool) (EtcdMembers, error) {
	members, membersErr := cli.getMembersWithRetries(cli.Retries)
	if (!statusInfo) || (membersErr != nil) {
		return members, membersErr
	}

	leaderId := uint64(0)
	raftTerm := uint64(0)
	for idx, member := range members.Members {
		status, statusErr := cli.getEndpointStatusWithRetries(member.ClientUrls[0], cli.Retries)
		if statusErr != nil {
			member.Status = &EtcdMemberStatus{
				IsResponsive: false,
				ResponseError: statusErr,
			}
			members.Members[idx] = member

			continue
		}

		member.Status = &EtcdMemberStatus{
			IsLeader: false,
			IsResponsive: true,
			ResponseError: nil,
			ProtocolVersion: status.Version,
			DbSize: status.DbSize,
			DbSizeInUse: status.DbSizeInUse,
			RaftIndex: status.RaftIndex,
			RaftTerm: status.RaftTerm,
			RaftAppliedIndex: status.RaftAppliedIndex,
		}
		member.IsLearner = status.IsLearner
		members.Members[idx] = member
		
		if status.RaftTerm >= raftTerm {
			raftTerm = status.RaftTerm
			leaderId = status.Leader
		}
	}

	for idx, member := range members.Members {
		if member.Id == leaderId {
			member.Status.IsLeader = true
			members.Members[idx] = member
		}
	}

	return members, membersErr
}

func (cli *EtcdClient) moveLeaderWithRetries(transfereeID uint64, retries uint64) error {
	ctx, cancel := context.WithTimeout(cli.Context, cli.RequestTimeout)
	defer cancel()

	_, err := cli.Client.MoveLeader(ctx, transfereeID)
	if err != nil {
		if !shouldRetry(err, retries) {
			return err
		}

		time.Sleep(cli.RetryInterval)
		return cli.moveLeaderWithRetries(transfereeID, retries - 1)
	}

	return nil
}

/*
Sets the leader status on the node with the given name.
If isLeader is true, the node will be elected leader if it isn't.
If isLeader is false, the leadership will be transfered to another node.
*/
func (cli *EtcdClient) SetLeaderStatus(name string, isLeader bool) error {
	members, membersErr := cli.GetMembers(true)
	if membersErr != nil {
		return membersErr
	}

	leaderFound := false
	eligibleIds := []uint64{}
	for _, member := range members.Members {
		if member.Name == name {
			if member.IsLearner {
				return errors.New(fmt.Sprintf("Cannot change leader status of node %s as it is a learner", name))
			}

			if !member.Status.IsResponsive {
				return errors.New(fmt.Sprintf("Cannot change leader status of node %s as it is not responsive", name))
			}

			if (isLeader && member.Status.IsLeader) || ((!isLeader) && (!member.Status.IsLeader)) {
				return nil
			}

			if !member.Status.IsLeader {
				eligibleIds = append(eligibleIds, member.Id)
			} else {
				leaderFound = true
			}
		} else if member.Status.IsLeader {
			leaderFound = true
		} else {
			if (!isLeader) && (!member.IsLearner) && member.Status.IsResponsive {
				eligibleIds = append(eligibleIds, member.Id)
			}
		}
	}

	if !leaderFound {
		return errors.New("Cannot change leader status as it requires a pre-existing leader and no such node can be reached")
	}

	if len(eligibleIds) == 0 {
		return errors.New("Cannot change leader status as no transfer candidate exists")
	}

	return cli.moveLeaderWithRetries(eligibleIds[0], cli.Retries)
}

/*
Forces a change of leader in the etcd cluster.
*/
func (cli *EtcdClient) ChangeLeader() error {
	members, membersErr := cli.GetMembers(true)
	if membersErr != nil {
		return membersErr
	}

	leaderFound := false
	eligibleIds := []uint64{}
	for _, member := range members.Members {
		if member.Status.IsLeader {
			leaderFound = true
		} else {
			if (!member.IsLearner) && member.Status.IsResponsive {
				eligibleIds = append(eligibleIds, member.Id)
			}
		}
	}

	if !leaderFound {
		return errors.New("Cannot change leader status as it requires a pre-existing leader and no such node can be reached")
	}

	if len(eligibleIds) == 0 {
		return errors.New("Cannot change leader status as no transfer candidate exists")
	}

	return cli.moveLeaderWithRetries(eligibleIds[0], cli.Retries)
}