package keypb

import (
	"errors"

	"github.com/Ferlab-Ste-Justine/etcd-sdk/client"
)

var (
	ErrKeyDiffRequestNotOverview = errors.New("First KeyDiff request was not an overview")
	ErrKeyDiffRequestNotChanges = errors.New("KeyDiff request after first was not change")
	ErrKeyDiffRequestOpsMiscount = errors.New("Number of operations in KeyDiff requests did not match overview")
	ErrInvalidChangeType = errors.New("Change type for one of the changes was not valid")
)

func IsApiContractError(err error) bool {
	return err == ErrKeyDiffRequestNotOverview || err == ErrKeyDiffRequestNotChanges || err == ErrKeyDiffRequestOpsMiscount || err == ErrInvalidChangeType
}

type KeyDiffResult struct {
	KeyDiff client.KeyDiff
	Error error
}

func ProcessSendKeyDiffRequests(reqCh <-chan *SendKeyDiffRequest) <-chan KeyDiffResult {
	inserts := int64(0)
	updates := int64(0)
	deletions := int64(0)
	diff := client.KeyDiff{
		Inserts: map[string]string{},
		Updates: map[string]string{},
		Deletions: []string{},
	}
	resCh := make(chan KeyDiffResult)

	go func() {
		defer close(resCh)
		req := <- reqCh

		overview, ok := req.KeyDiff.Content.(*KeyDiff_Overview)
		if !ok {
			resCh <- KeyDiffResult{Error: ErrKeyDiffRequestNotOverview}
			return
		}

		inserts = overview.Overview.Inserts
		updates = overview.Overview.Updates
		deletions = overview.Overview.Deletions

		for req := range reqCh {
			changes, ok := req.KeyDiff.Content.(*KeyDiff_Changes)
			if !ok {
				resCh <- KeyDiffResult{Error: ErrKeyDiffRequestNotChanges}
				return
			}

			for _, change := range changes.Changes.Changes {
				switch change.Type {
				case KeyDiffChangeType_INSERT:
					diff.Inserts[change.Key] = string(change.Value)
				case KeyDiffChangeType_UPDATE:
					diff.Updates[change.Key] = string(change.Value)
				case KeyDiffChangeType_DELETION:
					diff.Deletions = append(diff.Deletions, change.Key)
				default:
					resCh <- KeyDiffResult{Error: ErrInvalidChangeType}
					return
				}
			}
		}

		if int64(len(diff.Inserts)) != inserts || int64(len(diff.Updates)) != updates || int64(len(diff.Deletions)) != deletions {
			resCh <- KeyDiffResult{Error: ErrInvalidChangeType}
			return
		}

		resCh <- KeyDiffResult{KeyDiff: diff}
	}()

	return resCh
}

func getNewSendKeyDiffRequestCh() *SendKeyDiffRequest {
	return &SendKeyDiffRequest{
		KeyDiff: &KeyDiff{
			Content: &KeyDiff_Changes{
				Changes: &KeyDiffChanges{
					Changes: []*KeyDiffChange{},
				},
			},
		},
	}
}

func GenSendKeyDiffRequests(diff client.KeyDiff, maxChunkSize uint64, done <-chan struct{}) <-chan *SendKeyDiffRequest {
	send := make(chan *SendKeyDiffRequest)

	go func() {
		defer close(send)

		req := &SendKeyDiffRequest{
			KeyDiff: &KeyDiff{
				Content: &KeyDiff_Overview{
					Overview: &KeyDiffOverview{
						Inserts: int64(len(diff.Inserts)),
						Updates: int64(len(diff.Updates)),
						Deletions: int64(len(diff.Deletions)),
					},
				},
			},
		}

		send <- req

		sendSize := uint64(0)
		req = getNewSendKeyDiffRequestCh()
	
		for key, val := range diff.Inserts {
			largerThanChunk := (sendSize + uint64(len(key)) + uint64(len(val))) > maxChunkSize
			hasChanges := len(req.KeyDiff.Content.(*KeyDiff_Changes).Changes.Changes) > 0
			if largerThanChunk && hasChanges {
				select {
				case send <- req:
				case <-done:
					return
				}
	
				req = getNewSendKeyDiffRequestCh()
				sendSize = uint64(0)
			}
	
			req.KeyDiff.Content.(*KeyDiff_Changes).Changes.Changes = append(req.KeyDiff.Content.(*KeyDiff_Changes).Changes.Changes, &KeyDiffChange{
				Key: key,
				Value: []byte(val),
				Type: KeyDiffChangeType_INSERT,
			})
			sendSize = sendSize + uint64(len(key)) + uint64(len(val))
		}
	
		for key, val := range diff.Updates {
			largerThanChunk := (sendSize + uint64(len(key)) + uint64(len(val))) > maxChunkSize
			hasChanges := len(req.KeyDiff.Content.(*KeyDiff_Changes).Changes.Changes) > 0
			if largerThanChunk && hasChanges {
				select {
				case send <- req:
				case <-done:
					return
				}
	
				req = getNewSendKeyDiffRequestCh()
				sendSize = uint64(0)
			}
	
			req.KeyDiff.Content.(*KeyDiff_Changes).Changes.Changes = append(req.KeyDiff.Content.(*KeyDiff_Changes).Changes.Changes, &KeyDiffChange{
				Key: key,
				Value: []byte(val),
				Type: KeyDiffChangeType_UPDATE,
			})
			sendSize = sendSize + uint64(len(key)) + uint64(len(val))
		}
	
		for _, key := range diff.Deletions {
			largerThanChunk := (sendSize + uint64(len(key))) > maxChunkSize
			hasChanges := len(req.KeyDiff.Content.(*KeyDiff_Changes).Changes.Changes) > 0
			if largerThanChunk && hasChanges {
				select {
				case send <- req:
				case <-done:
					return
				}
	
				req = getNewSendKeyDiffRequestCh()
				sendSize = uint64(0)
			}
	
			req.KeyDiff.Content.(*KeyDiff_Changes).Changes.Changes = append(req.KeyDiff.Content.(*KeyDiff_Changes).Changes.Changes, &KeyDiffChange{
				Key: key,
				Type: KeyDiffChangeType_DELETION,
			})
			sendSize = sendSize + uint64(len(key))
		}
	
		if len(req.KeyDiff.Content.(*KeyDiff_Changes).Changes.Changes) > 0 {
			select {
			case send <- req:
			case <-done:
				return
			}
		}
	}()

	return send
}
