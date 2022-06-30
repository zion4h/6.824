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
	"math/rand"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role int

const (
	Follower  Role = 1
	Candidate Role = 2
	Leader    Role = 3
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// 常规
	role                 Role // 角色
	StartAt              time.Time
	electionTimeout      time.Duration
	heartbeatsTimeout    time.Duration
	electionTimeoutChan  chan bool
	heartbeatTimeoutChan chan bool
	onLeaderCond         *sync.Cond
	offLeaderCond        *sync.Cond

	//leaderId int // 领导id
	//lastReceiveHeart time.Time
	//heartbeatTimeout time.Duration
	// state
	currentTerm int      // 服务器看到的最新任期，初始化为0，
	votedFor    int      // candidateId
	log         []string // entry数组，每个entry都包含一个对状态机的操作，以及leader收到该操作时的任期（从1开始）
	commitIndex int      // log中已提交entry的最大索引（提交？
	lastApplied int      // log中已应用entry的最大索引
	nextIndex   []int    // leader专属：内容是发送给每个server的下一个log的索引
	matchIndex  []int    // leader专属：每个server已经复制log的最高索引

	//State     int
	//lastCheck time.Time
	//LeaderId  int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isLeader := rf.role == Leader
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int // 候选人最新log的下标
	LastLogTerm  int // 候选人最新log对应term
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		*reply = RequestVoteReply{
			Term:        rf.currentTerm,
			VoteGranted: false,
		}
		return
	}

	// TODO:
	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) || args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.ChangeRole(Follower)
		rf.ResetTicker()

		*reply = RequestVoteReply{
			Term:        rf.currentTerm,
			VoteGranted: true,
		}
	} else {
		// leader或者其他candidate拒绝
		*reply = RequestVoteReply{
			Term:        rf.currentTerm,
			VoteGranted: false,
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int //
	LeaderId     int //
	PrevLogIndex int //
	PrevLogTerm  int //
	Entries      []string
	LeaderCommit string
}

type AppendEntriesReply struct {
	Term    int  //
	Success bool //
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 2A主要还是实现Reply false if term < currentTerm
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		*reply = AppendEntriesReply{
			Term:    rf.currentTerm,
			Success: false,
		}
		return
	}

	rf.currentTerm = args.Term
	rf.ChangeRole(Follower)
	rf.ResetTicker()
	*reply = AppendEntriesReply{
		Term:    rf.currentTerm,
		Success: true,
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
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
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) Ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimeoutChan:
			go rf.startElection()
		case <-rf.heartbeatTimeoutChan:
			go rf.sendHeartbeat()
		}
	}
}

func (rf *Raft) electionTimeoutTicker() {
	for rf.killed() == false {
		if _, isLeader := rf.GetState(); isLeader {
			rf.mu.Lock()
			rf.offLeaderCond.Wait()
			rf.mu.Unlock()
		} else {
			rf.mu.Lock()
			if time.Since(rf.StartAt) >= time.Millisecond*rf.electionTimeout {
				rf.electionTimeoutChan <- true
			}
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 15) // 应比ele-t 小一个量级
		}
	}
}

func (rf *Raft) heartbeatTimeoutTicker() {
	for rf.killed() == false {
		if _, isLeader := rf.GetState(); isLeader {
			rf.mu.Lock()
			rf.heartbeatTimeoutChan <- true
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * time.Duration(100))
		} else {
			rf.mu.Lock()
			rf.onLeaderCond.Wait()
			rf.mu.Unlock()
		}
	}
}

// ResetTicker 重置计时器
func (rf *Raft) ResetTicker() {
	rand.Seed(time.Now().UnixNano())
	rf.StartAt = time.Now()
	if rf.role == Leader {
		rf.heartbeatsTimeout = 100 * time.Millisecond
	} else {
		rf.electionTimeout = time.Duration(150 + rand.Intn(150))
	}
}

func (rf *Raft) ChangeRole(toRole Role) {
	oldRole := rf.role
	rf.role = toRole
	if oldRole == Leader && toRole == Follower {
		rf.offLeaderCond.Broadcast()
	} else if oldRole == Candidate && toRole == Leader {
		rf.onLeaderCond.Broadcast()
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.ResetTicker()
	rf.ChangeRole(Candidate)
	nVotes := 1
	rf.mu.Unlock()

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		rf.mu.Lock()
		args := RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me,
		}
		var reply RequestVoteReply
		rf.mu.Unlock()

		go func(i int) {
			ok := rf.sendRequestVote(i, &args, &reply)
			if !ok {
				// do nothing.
			} else {
				if reply.VoteGranted {
					rf.mu.Lock()
					if rf.currentTerm == args.Term && rf.role == Candidate {
						nVotes++
						if nVotes > len(rf.peers)/2 {
							rf.votedFor = -1
							rf.ChangeRole(Leader)
							go rf.sendHeartbeat()
						}
					}
					rf.mu.Unlock()
				} else {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.ChangeRole(Follower)
					}
					rf.mu.Unlock()
				}
			}
		}(i)
	}
}

func (rf *Raft) sendHeartbeat() {

	if _, isLeader := rf.GetState(); !isLeader {
		return
	}

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		rf.mu.Lock()
		args := AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}
		rf.mu.Unlock()
		var reply AppendEntriesReply

		go func(server int) {
			// 如果发送失败就
			ok := rf.sendAppendEntries(server, &args, &reply)

			if !ok {
				return
			}

			if reply.Success {
				return
			}

			if reply.Term > args.Term {
				rf.mu.Lock()
				rf.currentTerm = reply.Term
				rf.ChangeRole(Follower)
				rf.votedFor = -1
				rf.ResetTicker()
				rf.mu.Unlock()
				return
			}
		}(i)
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// 2A
	// 初始没有领导，周期从0开始，也未投票给别人
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = Follower
	rf.electionTimeoutChan = make(chan bool)
	rf.heartbeatTimeoutChan = make(chan bool)
	rf.onLeaderCond = sync.NewCond(&rf.mu)
	rf.offLeaderCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.Ticker()
	go rf.electionTimeoutTicker()
	go rf.heartbeatTimeoutTicker()

	return rf
}
