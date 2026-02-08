package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// KvGet Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	// 创建响应对象
	resp := &kvrpcpb.GetResponse{}

	// 通过 Latches 上锁对应的 key
	server.Latches.WaitForLatches([][]byte{req.Key})
	defer server.Latches.ReleaseLatches([][]byte{req.Key})

	// 获取 Storage Reader
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Error = &kvrpcpb.KeyError{Abort: err.Error()}
		return resp, err
	}
	defer reader.Close()

	// 创建事务
	txn := mvcc.NewMvccTxn(reader, req.Version)

	// 获取 Lock
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		resp.Error = &kvrpcpb.KeyError{Abort: err.Error()}
		return resp, err
	}

	// 检查 Lock 是否存在，且 startTs 小于当前的 startTs
	if lock != nil && lock.Ts < req.Version {
		resp.Error = &kvrpcpb.KeyError{Locked: lock.Info(req.Key)}
		return resp, nil
	}

	// 获取 Value
	value, err := txn.GetValue(req.Key)
	if err != nil {
		resp.Error = &kvrpcpb.KeyError{Abort: err.Error()}
		return resp, err
	}
	if value == nil {
		resp.NotFound = true
	} else {
		resp.Value = value
	}

	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	// 创建响应对象
	resp := &kvrpcpb.PrewriteResponse{}

	// 通过 Latches 上锁对应的 keys
	keys := make([][]byte, len(req.Mutations))
	for i, mutation := range req.Mutations {
		keys[i] = mutation.Key
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	// 获取 Storage Reader
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Errors = []*kvrpcpb.KeyError{{Abort: err.Error()}}
		return resp, err
	}
	defer reader.Close()

	// 创建事务
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	for _, mutation := range req.Mutations {
		// 检查最新的 Write
		write, commitTs, err := txn.MostRecentWrite(mutation.Key)
		if err != nil {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Abort: err.Error()})
			continue
		}
		if write != nil && commitTs > req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Conflict: &kvrpcpb.WriteConflict{
				StartTs:    req.StartVersion,
				ConflictTs: commitTs,
				Key:        mutation.Key,
			}})
			continue
		}

		// 检查是否存在 Lock
		lock, err := txn.GetLock(mutation.Key)
		if err != nil {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Abort: err.Error()})
			continue
		}
		if lock != nil {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Locked: lock.Info(mutation.Key)})
			continue
		}

		// 写入 Default 数据和 Lock
		txn.PutValue(mutation.Key, mutation.Value)
		txn.PutLock(mutation.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    mvcc.WriteKindFromProto(mutation.Op),
		})
	}

	// 把 Default 和 Lock 写入存储
	err = server.storage.Write(req.Context, txn.Writes())
	return resp, err
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	// 创建响应对象
	resp := &kvrpcpb.CommitResponse{}

	// 通过 Latches 上锁对应的 keys
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	// 获取 Storage Reader
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Error = &kvrpcpb.KeyError{Abort: err.Error()}
		return resp, err
	}
	defer reader.Close()

	// 创建事务
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	for _, key := range req.Keys {
		// 获取 Lock
		lock, err := txn.GetLock(key)
		if err != nil {
			resp.Error = &kvrpcpb.KeyError{Abort: err.Error()}
			return resp, err
		}

		// 检查 Lock 是否存在且 startTs 和当前事务的 startTs 一致
		if lock == nil {
			// 检查已经存在的 Write，可能是重复提交
			existingWrite, _, err := txn.CurrentWrite(key)
			if err != nil {
				resp.Error = &kvrpcpb.KeyError{Abort: err.Error()}
				return resp, err
			}
			// 存在Write
			if existingWrite != nil {
				if existingWrite.Kind == mvcc.WriteKindRollback {
					// 如果是回滚类型的，返回error
					resp.Error = &kvrpcpb.KeyError{
						Abort: "Transaction has been rolled back",
					}
					return resp, nil
				}
				if existingWrite.StartTS == req.StartVersion {
					// 已经提交过，直接返回成功
					return resp, nil
				}
			}

			// Lock不存在且没有对应的Write记录，说明prewrite请求丢失了
			// 按照Percolator协议，这是一个no-op，跳过该key继续处理
			continue
		}

		if lock.Ts != req.StartVersion {
			resp.Error = &kvrpcpb.KeyError{
				Abort:     "startTs mismatch",
				Retryable: "true",
			}
			return resp, nil
		}

		// 写入 Write 并移除 Lock
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
	}

	err = server.storage.Write(req.Context, txn.Writes())
	return resp, err
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	// 创建响应对象
	resp := &kvrpcpb.ScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()

	// 创建事务
	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()

	kvPairs := make([]*kvrpcpb.KvPair, 0, req.Limit)
	for i := uint32(0); i < req.Limit; i++ {
		// 获取Write下一对key/value
		key, value, err := scanner.Next()
		if err != nil {
			return resp, err
		}

		// Next函数仅在迭代器无效的时候返回key=nil，可以作为结束break的条件
		if key == nil {
			break
		}

		// 处理删除类型的提交
		if value == nil {
			continue
		}

		// 检查是否有锁
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}
		if lock != nil && lock.Ts < txn.StartTS {
			// 如果存在锁，添加错误信息
			kvPairs = append(kvPairs, &kvrpcpb.KvPair{Error: &kvrpcpb.KeyError{Locked: lock.Info(key)}})
			continue
		}

		// 添加键值对
		kvPairs = append(kvPairs, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		})
	}
	resp.Pairs = kvPairs
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	// 构造响应对象
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	write, commitTs, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return resp, err
	}
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return resp, err
	}

	// 如果已经提交，直接返回
	if write != nil && write.Kind != mvcc.WriteKindRollback {
		resp.CommitVersion = commitTs
		return resp, nil
	}

	if lock == nil {
		// 如果已经回滚，返回无操作
		if write != nil && write.Kind == mvcc.WriteKindRollback {
			resp.Action = kvrpcpb.Action_NoAction
			return resp, nil
		} else {
			// 回滚标记
			txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
				StartTS: req.LockTs,
				Kind:    mvcc.WriteKindRollback,
			})
			err := server.storage.Write(req.Context, txn.Writes())
			if err != nil {
				return resp, err
			}
			resp.Action = kvrpcpb.Action_LockNotExistRollback
			return resp, nil
		}
	}

	// 检查锁是否超时
	curTs := mvcc.PhysicalTime(req.CurrentTs)
	lockTs := mvcc.PhysicalTime(lock.Ts)
	if curTs > lockTs && curTs-lockTs >= lock.Ttl {
		// 锁超时，清除锁并回滚
		txn.DeleteLock(req.PrimaryKey)
		txn.DeleteValue(req.PrimaryKey)
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		err := server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return resp, err
		}
		resp.Action = kvrpcpb.Action_TTLExpireRollback
		return resp, nil
	}

	// 锁未超时，返回无操作
	resp.Action = kvrpcpb.Action_NoAction
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	// 构造响应对象
	resp := &kvrpcpb.BatchRollbackResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	keys := req.Keys

	// 检查是否可以回滚
	for _, key := range keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return resp, err
		}
		if write != nil && write.Kind != mvcc.WriteKindRollback {
			resp.Error = &kvrpcpb.KeyError{
				Abort: "true",
			}
			return resp, nil
		}
	}

	// 执行回滚
	for _, key := range keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return resp, err
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}
		if lock != nil && lock.Ts != txn.StartTS {
			txn.PutWrite(key, txn.StartTS, &mvcc.Write{
				StartTS: txn.StartTS,
				Kind:    mvcc.WriteKindRollback,
			})
			continue
		}
		if write != nil && write.Kind == mvcc.WriteKindRollback {
			continue
		}
		txn.DeleteLock(key)
		txn.DeleteValue(key)
		txn.PutWrite(key, txn.StartTS, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    mvcc.WriteKindRollback,
		})
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	// 构造响应对象
	resp := &kvrpcpb.ResolveLockResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	iter := reader.IterCF(engine_util.CfLock)
	defer iter.Close()

	var keys [][]byte
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.KeyCopy(nil)
		value, err := item.ValueCopy(nil)
		if err != nil {
			return resp, err
		}
		lock, err := mvcc.ParseLock(value)
		if err != nil {
			return resp, err
		}
		if lock.Ts == txn.StartTS {
			keys = append(keys, key)
		}
	}

	if req.CommitVersion == 0 {
		// 全部回滚
		rollbackReq := &kvrpcpb.BatchRollbackRequest{
			Keys:         keys,
			StartVersion: txn.StartTS,
			Context:      req.Context,
		}
		rbResp, err := server.KvBatchRollback(nil, rollbackReq)
		if err != nil {
			return resp, err
		}
		resp.Error = rbResp.Error
		resp.RegionError = rbResp.RegionError
		return resp, nil
	} else {
		// 全部提交
		commitReq := &kvrpcpb.CommitRequest{
			Keys:          keys,
			StartVersion:  txn.StartTS,
			CommitVersion: req.CommitVersion,
			Context:       req.Context,
		}
		cmResp, err := server.KvCommit(nil, commitReq)
		if err != nil {
			return resp, err
		}
		resp.Error = cmResp.Error
		resp.RegionError = cmResp.RegionError
		return resp, nil
	}
}

// Coprocessor SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
