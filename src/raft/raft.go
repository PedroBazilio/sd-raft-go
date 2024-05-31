package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"raft/labrpc"
	"math/rand"
	"sync"
	"time"
)


type State int //node state: 0 -> follower, 1 -> candidate, 2 -> leader

const (
	Follower State = iota
	Candidate
	Leader
)

// Essential constants for the program
const (
	
	DefaultElectionTimeoutMin   = 250
	DefaultElectionTimeoutRange = 150
	DefaultHeartbeatInterval    = 41
	DefaultChannelBufferSize    = 23
)

//estrutura de log para os nós
type LogEntry struct {
	LogTerm int
	LogIndex int
	LogCommand interface{}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index        int
	Command      interface{}
	ValidCommand bool	// não sei se precisa
	UseSnapshot  bool   // ignore for lab2; only used in lab3
	Snapshot     []byte // ignore for lab2; only used in lab3
}

type AppendEntriesArgs struct {
    Term         int	//Leader's term number
    LeaderId     int	//Leader's id
    // PrevLogIndex int
    // PrevLogTerm  int
    // Entries      []LogEntry
    // LeaderCommit int
}

type AppendEntriesReply struct {
    Term    int
    Success bool
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term			int
	CandidateId		int
	// LastLogIndex	int
	// LastLogTerm	    int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term		int
	VoteGranted		bool
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's 
	state	  State
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	
	//Persistent state on all servers ((Updated on stable storage before responding to RPCs))
	currentTerm int
	votedFor    int		//candidateId that received a vote in current Term (null if none)
	votesCount 	int		//Count for received votes
	requestVoteReplied chan bool
	appendEntriesReceived 	chan bool
	winner chan bool
	alive	bool

	// //Volatile state on all servers
	// applyChannel     chan ApplyMsg
	// commitIndex int		//index of log w highest ranking known to be committed
	// lastApplied int		//index of log w highest ranking applied to state machine

	// //Volatile state on leaders (Reinitialized after election)
	// nextIndex   []int	//index of the next log entry to send to server (initialized w last logIndex + 1)
	// matchIndex  []int	//index of highest log entry the leader received from each follower

}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	return rf.currentTerm, rf.state == Leader

}

func (rf *Raft) GetActualState() State {

	//Guarantee that the data is thread safe to be accessed
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	return rf.state

}


func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// rf -> receiver
	// args -> sender

	// current args are in an old term and will not be appended
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		fmt.Printf("Server %d has sent an outdated message\n", args.LeaderId)
		return
	}

	rf.appendEntriesReceived <- true

	if args.Term > rf.currentTerm {

		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	
		reply.Term = rf.currentTerm
		if args.Term < rf.currentTerm {
			reply.Success = false
		}

		if reply.Success {
			if rf.alive {
				fmt.Printf("Message received correctly by: %d \n", rf.me)
			}
		} else {
			if rf.alive {
				fmt.Printf("Server %d has sent a failed message. \n", args.LeaderId)
			}
		}
	}

	
}


func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {	// rf = remetente   args = destinatario
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if ok {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				return ok
			}
		}
	return ok
}


//
// example RequestVote RPC handler.
// rf -> receiver | args -> sender

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false

	if args.Term < rf.currentTerm {		// se term remetente < destinatario, nao vota
		reply.Term = rf.currentTerm
		return
	}

	//case where the log is up-to-date isn't for 2A

	if args.Term > rf.currentTerm {		
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = args.CandidateId

	} else if rf.state != Follower {		// same term and and receiver isn't a follower: don't vote
		reply.Term = rf.currentTerm
		return
	} else if(rf.votedFor == -1 || rf.votedFor == args.CandidateId){ //  same term and and receiver is a follower: vote
		reply.Term = rf.currentTerm
		return
	}

	rf.votedFor = args.CandidateId
	reply.Term = rf.currentTerm
	reply.VoteGranted = true

	// Signal that a vote reply has been sent
	rf.requestVoteReplied <- true
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {

		//update term and state if reply term's is greate than the actual
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			fmt.Printf("%d isn't anymore a %d\n", rf.me,rf.GetActualState())
			rf.state= Follower
			rf.votedFor = -1
			return ok
		}
		
		if reply.VoteGranted {
			rf.votesCount++
			if rf.state == Candidate && rf.votesCount > (len(rf.peers) / 2){
				rf.winner <- true
			}
		}
	}

	return ok
}


func (rf *Raft) sendToAllRequestVote() {
	var args RequestVoteArgs
	rf.mu.Lock()
	args = RequestVoteArgs{
		Term:       rf.currentTerm,
		CandidateId: rf.me,
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.state == Candidate {

			if rf.alive {
				fmt.Printf("Node %d is asking for %d's votes on the Term %d.\n", rf.me, i, rf.currentTerm)
			}

			go func(i int) {
				var reply RequestVoteReply
				rf.sendRequestVote(i, &args, &reply)
			}(i)
		}
	}
}


func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	fmt.Printf("Server %d was killed!\n", rf.me)
	rf.alive = false
}


func (rf *Raft) performFollowerActions() {

	electionTimeout := rand.Intn(DefaultElectionTimeoutRange) + DefaultElectionTimeoutMin

	select {
	case <- time.After(time.Duration(electionTimeout) * time.Millisecond): ////didn't received heartbeats or didn't voted on the interval
		rf.mu.Lock()
		rf.state = Candidate
		rf.mu.Unlock()	
	case <-rf.requestVoteReplied: //voted
	case <-rf.appendEntriesReceived://received heartbeat
	}
}

func (rf *Raft) performCandidateActions() {

	rf.initiateCandidateState()
	rf.mu.Unlock()

	go rf.sendToAllRequestVote()
	rf.handleCandidateElectionTimeout()
    
}

func (rf *Raft) performLeaderActions() {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.sendHeartbeats()
	time.Sleep(DefaultHeartbeatInterval * time.Millisecond)
}

func (rf *Raft) sendHeartbeats() {
	for i := range rf.peers {
		if i != rf.me {
			var args = AppendEntriesArgs{
				Term:       rf.currentTerm,
				LeaderId: rf.me,
			}
			var reply AppendEntriesReply
			go rf.sendAppendEntries(i, args, &reply)
		}
	}
}


func (rf *Raft) initiateCandidateState() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votesCount = 1
	fmt.Printf("%d applied with %d votes\n", rf.me, rf.votesCount)
}

func (rf *Raft) handleCandidateElectionTimeout() {
	electionTimeout := rand.Intn(DefaultElectionTimeoutRange) + DefaultElectionTimeoutMin
	select {
	// 1 ->  the node win the election 
	// 2 ->  another node became leader 
	// 3 ->  a period of time passed without a winner 
	case <-rf.winner:
		rf.mu.Lock()
		defer rf.mu.Unlock()
		fmt.Printf("%d won with %d votes\n", rf.me, rf.votesCount)
		rf.state = Leader
	case <-rf.appendEntriesReceived:
	case <-time.After(time.Duration(electionTimeout) * time.Millisecond): 
	}
}

//Execute actions according the actual state of the node
func (rf *Raft) raftLoop() {
    stateActions := map[State]func(){
        Follower:  rf.performFollowerActions,
        Candidate: rf.performCandidateActions,
        Leader:    rf.performLeaderActions,
    }

    for {
        state := rf.GetActualState() // Get current state of node
        if action, exists := stateActions[state]; exists {
            action() 
        }
        time.Sleep(10 * time.Millisecond) 
    }
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	
rf.	initRaft(peers, me, persister, applyCh)
	go rf.raftLoop()

	return rf
}

func (rf *Raft) initRaft(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) {
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.alive = true

	// Initial state
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.votesCount = 0

	// Channels for communication
	rf.appendEntriesReceived = make(chan bool, DefaultChannelBufferSize)
	rf.requestVoteReplied = make(chan bool, DefaultChannelBufferSize)
	rf.winner = make(chan bool, DefaultChannelBufferSize)
}

