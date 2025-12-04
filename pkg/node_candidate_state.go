package pkg

import (
	"sync"
)

// doCandidate implements the logic for a Raft node in the candidate state.
func (n *Node) doCandidate() stateFunction {
	n.Out("Transitioning to CANDIDATE_STATE")
	n.setState(CandidateState)

	// TODO: Students should implement this method
	// Hint: perform any initial work, and then consider what a node in the
	// candidate state should do when it receives an incoming message on every
	// possible channel.
	// Increment term and vote for self
	newTerm := n.GetCurrentTerm() + 1
	n.SetCurrentTerm(newTerm)
	n.setVotedFor(n.Self.Id)
	n.Out("Starting election for term %v", newTerm)

	// Request votes from all peers
	fallback, electionResult := n.requestVotes(newTerm)
	if fallback {
		// Found a higher term, become follower
		return n.doFollower
	}
	if electionResult {
		// Won election, become leader
		return n.doLeader
	}

	// Election timeout, restart election
	electionTimeout := randomTimeout(n.Config.ElectionTimeout)
	for {
		select {
		case shutdown := <-n.gracefulExit:
			if shutdown {
				return nil
			}
		case msg := <-n.appendEntries:
			resetTimeout, fallback := n.handleAppendEntries(msg)
			if fallback {
				// Found a leader with equal or higher term, become follower
				return n.doFollower
			}
			if resetTimeout {
				// Reset timeout if we received a valid AppendEntries
				electionTimeout = randomTimeout(n.Config.ElectionTimeout)
			}
		case msg := <-n.requestVote:
			fallback := n.handleRequestVote(msg)
			if fallback {
				// Found a candidate with higher term, become follower
				return n.doFollower
			}
		case msg := <-n.clientRequest:
			// Candidate cannot handle client requests
			leader := n.getLeader()
			reply := ClientReply{
				Status:     ClientStatus_ELECTION_IN_PROGRESS,
				ClientId:   msg.request.ClientId,
				Response:   nil,
				LeaderHint: leader,
			}
			msg.reply <- reply
		case <-electionTimeout:
			// Election timeout, restart election
			n.Out("Election timeout, restarting election")
			return n.doCandidate
		}
	}
}

// requestVotes is called to request votes from all other nodes. It takes in a
// channel on which the result of the vote should be sent over: true for a
// successful election, false otherwise.
func (n *Node) requestVotes(currTerm uint64) (fallback, electionResult bool) {
	// TODO: Students should implement this method
	peers := n.getPeers()
	lastLogIndex := n.LastLogIndex()
	var lastLogTerm uint64
	if lastLogIndex > 0 {
		lastLogEntry := n.GetLog(lastLogIndex)
		if lastLogEntry != nil {
			lastLogTerm = lastLogEntry.TermId
		}
	}

	request := &RequestVoteRequest{
		Term:         currTerm,
		Candidate:    n.Self,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	votesGranted := 1 // Vote for self
	votesNeeded := (len(peers) / 2) + 1
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, peer := range peers {
		if peer.Id == n.Self.Id {
			continue // Skip self
		}

		wg.Add(1)
		go func(p *RemoteNode) {
			defer wg.Done()
			reply, err := p.RequestVoteRPC(n, request)
			if err != nil {
				n.Out("Error requesting vote from %v: %v", p.Id, err)
				return
			}

			mu.Lock()
			defer mu.Unlock()

			// If reply has higher term, we should fallback
			if reply.Term > currTerm {
				n.Out("Received reply with higher term %v from %v", reply.Term, p.Id)
				n.SetCurrentTerm(reply.Term)
				n.setVotedFor("")
				fallback = true
				return
			}

			// If we got a vote, increment counter
			if reply.VoteGranted {
				votesGranted++
				n.Out("Received vote from %v, total votes: %v/%v", p.Id, votesGranted, votesNeeded)
			}
		}(peer)
	}

	wg.Wait()

	// Check if we won the election
	if !fallback && votesGranted >= votesNeeded {
		n.Out("Won election with %v votes", votesGranted)
		electionResult = true
	}

	return
}

// handleRequestVote handles an incoming vote request. It returns true if the caller
// should fall back to the follower state, false otherwise.
func (n *Node) handleRequestVote(msg RequestVoteMsg) (fallback bool) {
	// TODO: Students should implement this method
	request := msg.request
	reply := msg.reply

	// If candidate term is lower than ours, reject
	if request.Term < n.GetCurrentTerm() {
		reply <- RequestVoteReply{
			Term:        n.GetCurrentTerm(),
			VoteGranted: false,
		}
		return false
	}

	// If candidate term is higher, update term and become follower
	if request.Term > n.GetCurrentTerm() {
		n.Out("Received RequestVote with higher term %v, updating term and becoming follower", request.Term)
		n.SetCurrentTerm(request.Term)
		n.setVotedFor("")
		fallback = true
	}

	// According to Raft paper: candidate and leader reject equal term in requestVote
	// Only followers can vote in the same term
	if request.Term == n.GetCurrentTerm() && n.GetState() != FollowerState {
		reply <- RequestVoteReply{
			Term:        n.GetCurrentTerm(),
			VoteGranted: false,
		}
		return fallback
	}

	// Check if we've already voted in this term
	votedFor := n.GetVotedFor()
	if votedFor != "" && votedFor != request.Candidate.Id {
		// Already voted for someone else
		reply <- RequestVoteReply{
			Term:        n.GetCurrentTerm(),
			VoteGranted: false,
		}
		return fallback
	}

	// Check if candidate's log is at least as up-to-date as ours
	// According to Raft paper §5.4.1: vote if candidate's log is at least as up-to-date
	ourLastLogIndex := n.LastLogIndex()
	var ourLastLogTerm uint64
	if ourLastLogIndex > 0 {
		ourLastLogEntry := n.GetLog(ourLastLogIndex)
		if ourLastLogEntry != nil {
			ourLastLogTerm = ourLastLogEntry.TermId
		}
	}

	// Candidate's log is more up-to-date if:
	// 1. Its last log term is greater than ours, OR
	// 2. Its last log term equals ours and its last log index is >= ours
	candidateLogUpToDate := (request.LastLogTerm > ourLastLogTerm) ||
		(request.LastLogTerm == ourLastLogTerm && request.LastLogIndex >= ourLastLogIndex)

	if candidateLogUpToDate {
		// Vote for candidate
		n.setVotedFor(request.Candidate.Id)
		reply <- RequestVoteReply{
			Term:        n.GetCurrentTerm(),
			VoteGranted: true,
		}
		n.Out("Voted for candidate %v in term %v", request.Candidate.Id, request.Term)
	} else {
		// Candidate's log is not up-to-date
		reply <- RequestVoteReply{
			Term:        n.GetCurrentTerm(),
			VoteGranted: false,
		}
	}

	return fallback
}
