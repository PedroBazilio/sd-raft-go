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
	"raft/labrpc"
	"sync"
	"time"
	"bytes"
	"encoding/gob"
)


type State int //node state: 0 -> follower, 1 -> candidate, 2 -> leader

const (
	Follower State = iota
	Candidate
	Leader
)

// Padrão de tempo para operações de eleição e 'batimentos'
const (
	DefaultElectionTimeoutMin   = 250
	DefaultElectionTimeoutRange = 150
	DefaultHeartbeatInterval    = 47
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
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's 
	state	  State
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead	  int32

	//Persistent state on all servers ((Updated on stable storage before responding to RPCs))
	currentTerm int
	votedFor    int		//candidateId that received a vote in current Term (null if none)
	votesCount 	int		//Count for received votes
	requestVoteReplied chan bool
	appendEntriesReceived 	chan bool
	// commandApplied 			chan ApplyMsg
	applyChannel     chan ApplyMsg

	winner chan bool


	// Utilizaremos?
	// //Volatile state on all servers
	// commitIndex int		//index of log w highest ranking known to be committed
	// lastApplied int		//index of log w highest ranking applied to state machine

	// //Volatile state on leaders (Reinitialized after election)
	// nextIndex   []int	//index of the next log entry to send to server (initialized w last logIndex + 1)
	// matchIndex  []int	//index of highest log entry the leader received from each follower

}


//Utilizaremos?
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	return rf.currentTerm, rf.state == Leader

}

func (rf *Raft) GetActualState() State {

	return rf.state

}


//Utilizaremos?
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	// e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//Utilizaremos?
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	// d.Decode(&rf.log)
	
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

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
	voteGranted		bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}


func (rf *Raft) performFollowerActions() {
    
}

func (rf *Raft) performCandidateActions() {
    
}

func (rf *Raft) performLeaderActions() {
    
}


func (rf *Raft) raftLoop() {
    stateActions := map[State]func(){
        Follower:  rf.performFollowerActions,
        Candidate: rf.performCandidateActions,
        Leader:    rf.performLeaderActions,
    }

    for {
        state := rf.GetActualState() // Get the current state of the node
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
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	
	rf.votesCount = 0

	rf.appendEntriesReceived = make(chan bool, DefaultChannelBufferSize)
	rf.requestVoteReplied = make(chan bool, DefaultChannelBufferSize)
	rf.winner = make(chan bool, DefaultChannelBufferSize)

	
	rf.raftLoop()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
