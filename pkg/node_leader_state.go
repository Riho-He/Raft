package pkg

import (
	"strconv"
	"strings"
	"sync"
	"time"
)

// doLeader implements the logic for a Raft node in the leader state.
func (n *Node) doLeader() stateFunction {
	n.Out("Transitioning to LeaderState")
	n.setState(LeaderState)

	// TODO: Students should implement this method
	// Hint: perform any initial work, and then consider what a node in the
	// leader state should do when it receives an incoming message on every
	// possible channel.
	// Initialize nextIndex and matchIndex for all peers
	peers := n.getPeers()
	lastLogIndex := n.LastLogIndex()
	n.LeaderMutex.Lock()
	for _, peer := range peers {
		if peer.Id != n.Self.Id {
			n.nextIndex[peer.Id] = lastLogIndex + 1
			n.matchIndex[peer.Id] = 0
		}
	}
	n.LeaderMutex.Unlock()
	n.Out("Initialized nextIndex and matchIndex for all peers")

	// Set self as leader
	n.setLeader(n.Self)

	// Each leader stores a blank no-op entry into the log at the start of its term
	noOpEntry := &LogEntry{
		Index:  lastLogIndex + 1,
		TermId: n.GetCurrentTerm(),
		Type:   CommandType_NOOP,
		Data:   []byte{},
	}
	n.StoreLog(noOpEntry)
	n.Out("Added no-op entry at index %v at start of term %v", noOpEntry.Index, n.GetCurrentTerm())

	// Send initial heartbeat immediately
	heartbeatTicker := time.NewTicker(n.Config.HeartbeatTimeout)
	defer heartbeatTicker.Stop()

	for {
		select {
		case shutdown := <-n.gracefulExit:
			if shutdown {
				return nil
			}
		case msg := <-n.appendEntries:
			_, fallback := n.handleAppendEntries(msg)
			if fallback {
				// Found a leader with equal or higher term, become follower
				return n.doFollower
			}
		case msg := <-n.requestVote:
			fallback := n.handleRequestVote(msg)
			if fallback {
				// Found a candidate with higher term, become follower
				return n.doFollower
			}
		case msg := <-n.clientRequest:
			// Handle client request
			req := msg.request
			cacheID := CreateCacheID(req.ClientId, req.SequenceNum)

			// Reject if not a leader (shouldn't happen, but safety check)
			if n.GetState() != LeaderState {
				leader := n.getLeader()
				reply := ClientReply{
					Status:     ClientStatus_NOT_LEADER,
					ClientId:   req.ClientId,
					Response:   nil,
					LeaderHint: leader,
				}
				msg.reply <- reply
				continue
			}

			// Check if this request was already processed and cached
			n.requestsMutex.Lock()
			if cachedReply, exists := n.GetCachedReply(cacheID); exists {
				n.requestsMutex.Unlock()
				msg.reply <- *cachedReply
				continue
			}

			// If an entry in n.requestsByCacheId corresponding to the cache ID already exists,
			// append the reply channel of the newly received request to the existing array.
			// This ensures when the reply is available, both old and new reply channels are replied to.
			if _, exists := n.requestsByCacheID[cacheID]; exists {
				n.requestsByCacheID[cacheID] = append(n.requestsByCacheID[cacheID], msg.reply)
				n.requestsMutex.Unlock()
				// Request already in log, just wait for it to be processed
				continue
			}

			// Otherwise, just add the reply channel to n.requestsByCacheId
			n.requestsByCacheID[cacheID] = []chan ClientReply{msg.reply}
			n.requestsMutex.Unlock()

			// Create log entry
			newLogIndex := n.LastLogIndex() + 1
			var entryType CommandType
			if req.ClientId == 0 && req.SequenceNum == 0 {
				// Client registration
				entryType = CommandType_CLIENT_REGISTRATION
			} else {
				entryType = CommandType_STATE_MACHINE_COMMAND
			}

			logEntry := &LogEntry{
				Index:   newLogIndex,
				TermId:  n.GetCurrentTerm(),
				Type:    entryType,
				Command: req.StateMachineCmd,
				Data:    req.Data,
				CacheId: cacheID,
			}

			// Append to log
			n.StoreLog(logEntry)
			n.Out("Added client request to log at index %v", newLogIndex)

			// Send heartbeats to replicate the log entry
			// Note: We send synchronously here to ensure the log entry is replicated
			// The periodic heartbeats are sent asynchronously
			fallback := n.sendHeartbeats()
			if fallback {
				// Found a higher term, become follower
				return n.doFollower
			}
		case <-heartbeatTicker.C:
			// Send periodic heartbeats (asynchronously for performance)
			go func() {
				_ = n.sendHeartbeats()
				// If fallback is true, it will be handled when we receive a message with higher term
			}()
		}
	}
}

// sendHeartbeats is used by the leader to send out heartbeats to each of
// the other nodes. It returns true if the leader should fall back to the
// follower state. (This happens if we discover that we are in an old term.)
//
// If another node isn't up-to-date, then the leader should attempt to
// update them, and, if an index has made it to a quorum of nodes, commit
// up to that index. Once committed to that index, the replicated state
// machine should be given the new log entries via processLogEntry.
func (n *Node) sendHeartbeats() (fallback bool) {
	// TODO: Students should implement this method
	peers := n.getPeers()
	currentTerm := n.GetCurrentTerm()
	lastLogIndex := n.LastLogIndex()

	var wg sync.WaitGroup
	var mu sync.Mutex
	fallback = false

	// Send AppendEntries to all peers
	for _, peer := range peers {
		if peer.Id == n.Self.Id {
			continue // Skip self
		}

		wg.Add(1)
		go func(p *RemoteNode) {
			defer wg.Done()

			// Get nextIndex for this peer
			n.LeaderMutex.Lock()
			nextIdx := n.nextIndex[p.Id]
			if nextIdx == 0 {
				// Initialize nextIndex
				nextIdx = lastLogIndex + 1
				n.nextIndex[p.Id] = nextIdx
			}
			n.LeaderMutex.Unlock()

			// Prepare AppendEntries request
			var prevLogIndex uint64
			var prevLogTerm uint64
			var entries []*LogEntry

			if nextIdx > 0 {
				prevLogIndex = nextIdx - 1
				if prevLogIndex > 0 {
					prevLogEntry := n.GetLog(prevLogIndex)
					if prevLogEntry != nil {
						prevLogTerm = prevLogEntry.TermId
					}
				}
			}

			// Get entries to send (from nextIdx to lastLogIndex)
			// Unlike the paper, we try to bring followers' log up to date in each heartbeat
			// We don't send empty AppendEntriesRPC as heartbeats (unless nextIndex already exceeds lastLogIndex)
			if nextIdx <= lastLogIndex {
				for i := nextIdx; i <= lastLogIndex; i++ {
					entry := n.GetLog(i)
					if entry != nil {
						entries = append(entries, entry)
					}
				}
			}
			// If nextIdx > lastLogIndex, entries will be empty, which is fine for heartbeat

			request := &AppendEntriesRequest{
				Term:         currentTerm,
				Leader:       n.Self,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: n.CommitIndex.Load(),
			}

			// Send AppendEntries RPC
			reply, err := p.AppendEntriesRPC(n, request)
			if err != nil {
				n.Out("Error sending AppendEntries to %v: %v", p.Id, err)
				return
			}

			mu.Lock()
			defer mu.Unlock()

			// Check if reply has higher term
			if reply.Term > currentTerm {
				n.Out("Received AppendEntries reply with higher term %v from %v", reply.Term, p.Id)
				n.SetCurrentTerm(reply.Term)
				n.setVotedFor("")
				fallback = true
				return
			}

			// Update nextIndex and matchIndex based on reply
			if reply.Success {
				// Successfully replicated entries
				if len(entries) > 0 {
					newNextIdx := nextIdx + uint64(len(entries))
					n.LeaderMutex.Lock()
					n.nextIndex[p.Id] = newNextIdx
					n.matchIndex[p.Id] = newNextIdx - 1
					n.LeaderMutex.Unlock()
					n.Out("Successfully replicated entries to %v, nextIndex=%v, matchIndex=%v", p.Id, newNextIdx, newNextIdx-1)
				} else {
					// Heartbeat successful, update matchIndex if needed
					n.LeaderMutex.Lock()
					if nextIdx > 0 && n.matchIndex[p.Id] < nextIdx-1 {
						n.matchIndex[p.Id] = nextIdx - 1
					}
					n.LeaderMutex.Unlock()
				}
			} else {
				// Failed, decrement nextIndex and retry
				n.LeaderMutex.Lock()
				if n.nextIndex[p.Id] > 1 {
					n.nextIndex[p.Id]--
				} else {
					n.nextIndex[p.Id] = 1
				}
				n.LeaderMutex.Unlock()
				n.Out("Failed to replicate to %v, decrementing nextIndex to %v", p.Id, n.nextIndex[p.Id])
			}
		}(peer)
	}

	wg.Wait()

	// If we found a higher term, return fallback
	if fallback {
		return true
	}

	// Check if any log entries can be committed
	// Find the highest index that has been replicated to a majority

	// Sort matchIndices to find median (simplified: find majority)
	// For each index from commitIndex+1 to lastLogIndex, check if it's replicated to majority
	currentCommitIndex := n.CommitIndex.Load()
	for idx := currentCommitIndex + 1; idx <= lastLogIndex; idx++ {
		// Count how many nodes have replicated this index
		replicatedCount := 1 // Leader always has it
		n.LeaderMutex.Lock()
		for _, matchIdx := range n.matchIndex {
			if matchIdx >= idx {
				replicatedCount++
			}
		}
		n.LeaderMutex.Unlock()

		// Check if majority has replicated this index
		majority := (len(peers) / 2) + 1
		if replicatedCount >= majority {
			// Check if this entry is from current term (safety requirement)
			entry := n.GetLog(idx)
			if entry != nil && entry.TermId == currentTerm {
				// Can commit this index
				if idx > currentCommitIndex {
					n.Out("Committing log index %v (replicated to %v/%v nodes)", idx, replicatedCount, len(peers))
					n.CommitIndex.Store(idx)
					// Process log entry
					for n.CommitIndex.Load() > n.LastApplied.Load() {
						n.processLogEntry(n.LastApplied.Inc())
					}
				}
			}
		}
	}

	return false
}

// processLogEntry applies a single log entry to the finite state machine. It is
// called once a log entry has been replicated to a majority and committed by
// the leader. Once the entry has been applied, the leader responds to the client
// with the result, and also caches the response.
func (n *Node) processLogEntry(logIndex uint64) (fallback bool) {
	fallback = false
	entry := n.GetLog(logIndex)
	n.Out("Processing log index: %v, entry: %v", logIndex, entry)

	status := ClientStatus_OK
	var response []byte
	var err error
	var clientId uint64

	switch entry.Type {
	case CommandType_NOOP:
		return
	case CommandType_CLIENT_REGISTRATION:
		clientId = logIndex
	case CommandType_STATE_MACHINE_COMMAND:
		if clientId, err = strconv.ParseUint(strings.Split(entry.GetCacheId(), "-")[0], 10, 64); err != nil {
			panic(err)
		}
		if resp, ok := n.GetCachedReply(entry.GetCacheId()); ok {
			status = resp.GetStatus()
			response = resp.GetResponse()
		} else {
			response, err = n.StateMachine.ApplyCommand(entry.Command, entry.Data)
			if err != nil {
				status = ClientStatus_REQ_FAILED
				response = []byte(err.Error())
			}
		}
	}

	// Construct reply
	reply := ClientReply{
		Status:     status,
		ClientId:   clientId,
		Response:   response,
		LeaderHint: &RemoteNode{Addr: n.Self.Addr, Id: n.Self.Id},
	}

	// Send reply to client
	n.requestsMutex.Lock()
	defer n.requestsMutex.Unlock()
	// Add reply to cache
	if entry.CacheId != "" {
		if err = n.CacheClientReply(entry.CacheId, reply); err != nil {
			panic(err)
		}
	}
	if replies, exists := n.requestsByCacheID[entry.CacheId]; exists {
		for _, ch := range replies {
			ch <- reply
		}
		delete(n.requestsByCacheID, entry.CacheId)
	}

	return
}
