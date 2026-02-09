// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64 // 应用到状态机的最大日志条目的索引，系统的状态已经根据这些日志条目进行了更新

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64 // 已经持久化到存储中的最大日志条目的索引

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	// 确保存储不为 nil
	if storage == nil {
		log.Panicf("storage must not be nil")
	}

	// 获取第一个日志索引
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		log.Panicf("failed to get first index from storage: %v", err)
	}

	// 获取最后一个日志索引
	lastIndex, err := storage.LastIndex()
	if err != nil {
		log.Panicf("failed to get last index from storage: %v", err)
	}

	// 获取从 firstIndex 到 lastIndex+1 的所有日志条目
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err == ErrUnavailable {
		// Persisted raft state may temporarily be ahead of local raft log files
		// (e.g. after split/recover races). Start from empty in-memory entries
		// and rely on snapshot/replication to heal.
		entries = nil
	} else if err != nil {
		log.Panicf("failed to get entries from storage: %v", err)
	}

	// 获取初始状态，包括硬状态和配置状态
	hardState, _, err := storage.InitialState()
	if err != nil {
		log.Panicf("failed to get initial state from storage: %v", err)
	}

	// 初始化 RaftLog 实例
	raftLog := &RaftLog{
		storage:         storage,
		committed:       hardState.Commit, // 从硬状态中获取已提交的索引
		applied:         firstIndex - 1,   // 初始化 applied 索引，表示应用到状态机的最新索引
		stabled:         lastIndex,        // 初始化 stabled 索引，表示已持久化到存储的最新索引
		entries:         entries,          // 初始化日志条目
		pendingSnapshot: nil,              // 初始化挂起的快照为空
	}

	return raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	// 获取已被压缩的日志的下一个索引，即持久化的第一个索引
	compacted, err := l.storage.FirstIndex()
	if err != nil {
		log.Errorf("Could not find lastindex in storage")
		return
	}
	if len(l.entries) > 0 {
		if compacted > l.LastIndex() {
			// 舍弃整个entry
			l.entries = []pb.Entry{}
			return
		} else if compacted >= l.FirstIndex() {
			// 舍弃前面部分
			l.entries = l.entries[compacted-l.FirstIndex():]
			return
		} else {
			// 不用舍弃
			return
		}
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	// TODO 不太理解什么是 dummy entries
	if len(l.entries) == 0 {
		return []pb.Entry{}
	}
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return []pb.Entry{}
	}

	// 找到未持久化的条目范围
	firstIndex := l.FirstIndex()
	if l.stabled < firstIndex {
		return l.entries
	}
	if l.stabled-firstIndex >= uint64(len(l.entries)-1) {
		return []pb.Entry{}
	}
	return l.entries[l.stabled-firstIndex+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	firstIndex := l.FirstIndex()
	appliedIndex := l.applied
	committedIndex := l.committed

	if len(l.entries) == 0 {
		return []pb.Entry{}
	}

	// 找到已提交但未应用的条目范围
	if appliedIndex >= firstIndex-1 && committedIndex >= firstIndex-1 && appliedIndex < committedIndex && committedIndex <= l.LastIndex() {
		return l.entries[appliedIndex-firstIndex+1 : committedIndex-firstIndex+1]
	}
	return []pb.Entry{}
}

// FirstIndex return the first index of the log entries
func (l *RaftLog) FirstIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		index, _ := l.storage.FirstIndex()
		return index
	}
	return l.entries[0].Index
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	// 如果 entries 为空，则表示所有的日志条目都已经被压缩到 snapshot 中
	if len(l.entries) == 0 {
		// 从持久化存储中获取最后一个日志条目的索引
		Index, _ := l.storage.LastIndex()
		return Index
	}

	// 返回当前 entries 中最后一个日志条目的索引
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// 如果内存中的日志条目不为空
	if len(l.entries) > 0 {
		// 获取内存中日志条目的第一个索引和最后一个索引
		firstIndex := l.FirstIndex()
		lastIndex := l.LastIndex()

		// 检查给定的索引是否在内存中的日志条目范围内
		if i >= firstIndex && i <= lastIndex {
			// 如果在范围内，返回对应的日志条目的任期
			return l.entries[i-firstIndex].Term, nil
		}
	}

	// 如果不在内存中的日志条目范围内，检查持久化存储
	term, err := l.storage.Term(i)
	if err == nil {
		return term, nil
	}
	return 0, err
}
