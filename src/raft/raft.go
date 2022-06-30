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

type Role int

const (
	Follower  Role = 1
	Candidate Role = 2
	Leader    Role = 3
)

// ApplyMsg
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

type Entry struct {
	Term    int         // 收到操作的周期
	Command interface{} // 操作状态机的命令
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	role                 Role // 角色
	StartAt              time.Time
	electionTimeout      time.Duration
	heartbeatsTimeout    time.Duration
	electionTimeoutChan  chan bool
	heartbeatTimeoutChan chan bool
	onLeaderCond         *sync.Cond
	offLeaderCond        *sync.Cond

	currentTerm int
	votedFor    int
	log         []Entry // entry数组，每个entry都包含一个对状态机的操作，以及leader收到该操作时的任期（从1开始，因为周期0没领导）
	commitIndex int     // log中已提交entry的最大索引
	lastApplied int     // log中已应用entry的最大索引
	nextIndex   []int   // leader专属：内容是发送给每个server的下一个log的索引，一般来新领导会初始化所有节点为为len(leader.log)
	matchIndex  []int   // leader专属：每个server已经复制log的最高索引

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

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// had more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int // 候选人最新log的下标
	LastLogTerm  int // 候选人最新log对应term
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		*reply = RequestVoteReply{
			Term:        rf.currentTerm,
			VoteGranted: false,
		}
		return
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) || args.Term > rf.currentTerm {
		lastLogIndex := len(rf.log) - 1
		lastLogTerm := rf.log[lastLogIndex].Term

		// If votedFor is null or candidateId, and candidate’s log is at
		// least as up-to-date as receiver’s log, grant vote
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			rf.ChangeRole(Follower)
			rf.ResetTicker()

			*reply = RequestVoteReply{
				Term:        rf.currentTerm,
				VoteGranted: true,
			}
			return
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
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
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

	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		*reply = AppendEntriesReply{
			Term:    rf.currentTerm,
			Success: false,
		}
		return
	}

	// commitIndex决定了当前节点状态机的状态
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < len(rf.log)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log) - 1
		}
	}

	// 论文中先检测是否一致，不一致则删除替换，我这里直接替换了
	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start leader的同步之路
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if term, isLeader = rf.GetState(); !isLeader {
		return index, term, isLeader
	}

	// 加入command到log
	rf.mu.Lock()
	index = len(rf.log)
	rf.log = append(rf.log, Entry{
		Term:    rf.currentTerm,
		Command: command,
	})
	cnt := 1
	rf.mu.Unlock()

	// 向所有节点发送append消息同步log
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(i int) {
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: index - 1,
				PrevLogTerm:  rf.log[index-1].Term,
				Entries:      []Entry{{Term: term, Command: command}},
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			var reply AppendEntriesReply

			// 如果追随者崩溃或运行缓慢，或者网络数据包丢失，领导者会无限期地重试附加条目 RPC（即使它已经响应客户端），直到所有追随者最终存储所有日志条目
			for {
				ok := rf.sendAppendEntries(i, &args, &reply)

				if ok {
					if reply.Success {
						rf.mu.Lock()
						if args.Term == rf.currentTerm && rf.role == Leader {
							cnt++

							if cnt > len(rf.peers)/2 {
								if index > rf.commitIndex {
									rf.commitIndex = index
								}
							}
						}
						rf.mu.Unlock()
						return
					} else {
						rf.mu.Lock()
						if args.Term == rf.currentTerm && rf.role == Leader {
							// 1.发给高term节点，变follower
							// 2.log不一致 往前找直到下标0，下标0一定是空的
							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
								rf.ChangeRole(Follower)
								rf.votedFor = -1
								rf.ResetTicker()
								rf.mu.Unlock()
								return
							} else {
								idx := args.PrevLogIndex - 1
								args.PrevLogIndex = idx
								args.PrevLogTerm = rf.log[idx].Term
								args.Entries = rf.log[idx+1:]
							}
						} else {
							rf.mu.Unlock()
							return
						}
						rf.mu.Unlock()
					}

				} else {
					// 网络错误，检测leader状态正常则不断重试
					rf.mu.Lock()
					if args.Term != rf.currentTerm || rf.role != Leader {
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
				}
			}

		}(i)
	}

	return index, term, isLeader
}

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
			time.Sleep(time.Millisecond * 10) // 应比ele-t小一个量级，hint提示用10ms
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

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		rf.mu.Lock()
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.log) - 1,
			LastLogTerm:  rf.log[len(rf.log)-1].Term,
		}
		var reply RequestVoteReply
		rf.mu.Unlock()

		go func(i int) {
			ok := rf.sendRequestVote(i, &args, &reply)
			if ok {
				if reply.VoteGranted {
					rf.mu.Lock()
					if rf.currentTerm == args.Term && rf.role == Candidate {
						nVotes++
						if nVotes > len(rf.peers)/2 {
							rf.votedFor = -1
							rf.ChangeRole(Leader)
							// 当领导者首次上台时，它会将所有 nextIndex 值初始化为其日志中最后一个值之后的索引
							for j := range rf.peers {
								rf.nextIndex[j] = len(rf.log)
							}
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

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		rf.mu.Lock()
		index := len(rf.log) - 1
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: index,
			PrevLogTerm:  rf.log[index].Term,
			Entries:      []Entry{},
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()
		var reply AppendEntriesReply

		go func(server int) {
			// 检测到一致性冲突就重复提交请求直到一致
			for {
				ok := rf.sendAppendEntries(server, &args, &reply)

				if !ok {
					// 网络错误则无视，判定失败
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

				rf.mu.Lock()
				if rf.currentTerm == args.Term && rf.role == Leader {
					conflictTerm := args.Term
					idx := args.PrevLogIndex
					for ; idx > 0 && rf.log[idx].Term == conflictTerm; idx-- {
					}
					args.PrevLogIndex = idx
					args.PrevLogTerm = rf.log[idx].Term
					args.Entries = rf.log[idx+1:]
				} else {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) checkAppliedLogTicker(applyCh chan ApplyMsg) {
	for {
		rf.mu.Lock()
		if rf.commitIndex > len(rf.log)-1 {
			rf.commitIndex = len(rf.log) - 1
		}
		// 将未应用命令给执行了
		for rf.lastApplied < rf.commitIndex {
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied+1].Command,
				CommandIndex: rf.lastApplied + 1,
			}
			applyCh <- applyMsg
			rf.lastApplied++

		}

		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 20)
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
	atomic.StoreInt32(&rf.dead, 0) // 1 是死掉 0 是存活
	// 初始没有领导，周期从0开始，也未投票给别人
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = Follower
	rf.electionTimeoutChan = make(chan bool)
	rf.heartbeatTimeoutChan = make(chan bool)
	rf.onLeaderCond = sync.NewCond(&rf.mu)
	rf.offLeaderCond = sync.NewCond(&rf.mu)

	// 2B
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.Ticker()
	go rf.electionTimeoutTicker()
	go rf.heartbeatTimeoutTicker()

	go rf.checkAppliedLogTicker(applyCh)
	return rf
}
