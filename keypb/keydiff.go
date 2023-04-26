package keypb

import (
	"github.com/Ferlab-Ste-Justine/etcd-sdk/client"
)

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
