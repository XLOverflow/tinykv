package raftstore

import (
	"fmt"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

// HandleRaftReady processes Raft Ready state:
// 1. Save persistent state (entries, hard state, snapshot) to DB
// 2. Send messages to other peers
// 3. Apply committed entries to KV state machine
// 4. Advance the Raft state machine
func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	if !d.RaftGroup.HasReady() {
		return
	}

	ready := d.RaftGroup.Ready()

	// Save persistent state (entries, hard state, snapshot)
	applySnapResult, err := d.peerStorage.SaveReadyState(&ready)
	if err != nil {
		return
	}

	// Update store metadata if snapshot was applied
	if applySnapResult != nil {
		if !reflect.DeepEqual(applySnapResult.PrevRegion, applySnapResult.Region) {
			d.peerStorage.SetRegion(applySnapResult.Region)
			d.ctx.storeMeta.Lock()
			d.ctx.storeMeta.regions[applySnapResult.Region.Id] = applySnapResult.Region
			d.ctx.storeMeta.regionRanges.Delete(&regionItem{region: applySnapResult.PrevRegion})
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: applySnapResult.Region})
			d.ctx.storeMeta.Unlock()
		}
	}

	// Send Raft messages
	d.Send(d.ctx.trans, ready.Messages)

	// Apply committed entries
	for _, entry := range ready.CommittedEntries {
		kvWB := new(engine_util.WriteBatch)

		if entry.EntryType == pb.EntryType_EntryNormal {
			d.applyNormalEntry(&entry, kvWB)
		} else if entry.EntryType == pb.EntryType_EntryConfChange {
			d.applyConfChangeEntry(&entry, kvWB)
		}

		// Update applied index
		d.peerStorage.applyState.AppliedIndex = entry.Index

		// If peer was destroyed (e.g., remove-self), stop processing
		if d.stopped {
			return
		}

		// Persist applyState to DB - critical for correct snapshot generation
		kvWB.SetMeta(meta.ApplyStateKey(d.Region().GetId()), d.peerStorage.applyState)
		kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
	}

	// Advance Raft state machine
	d.RaftGroup.Advance(ready)
}

// ====================== ConfChange Application ======================

func (d *peerMsgHandler) applyConfChangeEntry(entry *pb.Entry, kvWB *engine_util.WriteBatch) {
	// Deserialize ConfChange from entry.Data
	var cc pb.ConfChange
	if err := cc.Unmarshal(entry.Data); err != nil {
		log.Errorf("failed to unmarshal confchange: %v", err)
		return
	}

	// Deserialize RaftCmdRequest from ConfChange.Context
	var cmdRequest raft_cmdpb.RaftCmdRequest
	if err := cmdRequest.Unmarshal(cc.Context); err != nil {
		log.Errorf("failed to unmarshal confchange context: %v", err)
		return
	}

	// Check epoch staleness: skip stale confchanges
	if cmdRequest.Header != nil && cmdRequest.Header.RegionEpoch != nil {
		regionEpoch := d.Region().GetRegionEpoch()
		if util.IsEpochStale(cmdRequest.Header.RegionEpoch, regionEpoch) {
			d.RaftGroup.ApplyConfChange(pb.ConfChange{})
			d.checkValidAndCallback(&raft_cmdpb.RaftCmdResponse{
				Header: &raft_cmdpb.RaftResponseHeader{},
			}, entry)
			return
		}
	}

	removeSelf := false
	// Apply the conf change to region state
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		d.applyAddNode(&cc, &cmdRequest, kvWB)
	case pb.ConfChangeType_RemoveNode:
		removeSelf = d.applyRemoveNode(&cc, kvWB)
	}

	// Persist RegionState to DB
	kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
	kvWB.Reset()

	// Apply to Raft layer (update Prs)
	d.RaftGroup.ApplyConfChange(cc)
	if removeSelf {
		d.destroyPeer()
		return
	}

	// Notify scheduler about region change
	d.notifyHeartbeatScheduler(d.Region(), d.peer)

	// Callback
	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerResponse{},
		},
	}
	d.checkValidAndCallback(cmdResp, entry)
}

func (d *peerMsgHandler) applyAddNode(cc *pb.ConfChange, req *raft_cmdpb.RaftCmdRequest, kvWB *engine_util.WriteBatch) {
	if d.peerExistInRegion(cc.NodeId) {
		return
	}

	d.ctx.storeMeta.Lock()
	defer d.ctx.storeMeta.Unlock()

	region := d.Region()
	region.RegionEpoch.ConfVer++

	newPeer := &metapb.Peer{
		Id:      req.AdminRequest.ChangePeer.Peer.Id,
		StoreId: req.AdminRequest.ChangePeer.Peer.StoreId,
	}
	region.Peers = append(region.GetPeers(), newPeer)

	meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
	d.insertPeerCache(newPeer)
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
}

func (d *peerMsgHandler) applyRemoveNode(cc *pb.ConfChange, kvWB *engine_util.WriteBatch) bool {
	if !d.peerExistInRegion(cc.NodeId) {
		return false
	}

	d.ctx.storeMeta.Lock()
	defer d.ctx.storeMeta.Unlock()

	region := d.Region()
	region.RegionEpoch.ConfVer++

	newPeers := make([]*metapb.Peer, 0, len(region.Peers)-1)
	for _, pr := range region.Peers {
		if pr.Id != cc.NodeId {
			newPeers = append(newPeers, pr)
		}
	}
	region.Peers = newPeers

	meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
	d.removePeerCache(cc.NodeId)
	return d.peer.PeerId() == cc.NodeId
}

// ====================== Normal Entry Application ======================

func (d *peerMsgHandler) applyNormalEntry(entry *pb.Entry, kvWB *engine_util.WriteBatch) {
	var cmdRequest raft_cmdpb.RaftCmdRequest
	if err := cmdRequest.Unmarshal(entry.Data); err != nil {
		log.Errorf("failed to unmarshal normal entry: %v", err)
		return
	}

	// Handle admin requests
	if cmdRequest.AdminRequest != nil {
		d.applyAdminRequest(entry, &cmdRequest, kvWB)
		return
	}

	// Check epoch for normal requests (key might have been split to another region)
	if cmdRequest.Header != nil && cmdRequest.Header.RegionEpoch != nil {
		if err := util.CheckRegionEpoch(&cmdRequest, d.Region(), true); err != nil {
			d.checkValidAndCallback(ErrResp(err), entry)
			return
		}
	}

	// Handle normal KV requests
	for _, request := range cmdRequest.GetRequests() {
		switch request.CmdType {
		case raft_cmdpb.CmdType_Get:
			d.applyGetReq(entry, request)
		case raft_cmdpb.CmdType_Put:
			d.applyPutReq(entry, request, kvWB)
		case raft_cmdpb.CmdType_Delete:
			d.applyDeleteReq(entry, request, kvWB)
		case raft_cmdpb.CmdType_Snap:
			d.applySnapReq(entry, &cmdRequest)
		}
	}
}

func (d *peerMsgHandler) applyAdminRequest(entry *pb.Entry, cmdRequest *raft_cmdpb.RaftCmdRequest, kvWB *engine_util.WriteBatch) {
	adminReq := cmdRequest.GetAdminRequest()
	switch adminReq.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		d.applyCompactLog(entry, adminReq, kvWB)
	case raft_cmdpb.AdminCmdType_Split:
		d.applySplit(entry, cmdRequest, kvWB)
	}
}

func (d *peerMsgHandler) applyCompactLog(entry *pb.Entry, req *raft_cmdpb.AdminRequest, wb *engine_util.WriteBatch) {
	compactIndex := req.GetCompactLog().CompactIndex

	if compactIndex >= d.peerStorage.applyState.TruncatedState.Index {
		d.peerStorage.applyState.TruncatedState.Index = compactIndex
		d.peerStorage.applyState.TruncatedState.Term = req.GetCompactLog().CompactTerm
		wb.SetMeta(meta.ApplyStateKey(d.Region().GetId()), d.peerStorage.applyState)
		wb.MustWriteToDB(d.peerStorage.Engines.Kv)
		wb.Reset()
		d.ScheduleCompactLog(compactIndex)
	}

	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_CompactLog,
			CompactLog: &raft_cmdpb.CompactLogResponse{},
		},
	}
	d.checkValidAndCallback(cmdResp, entry)
}

// ====================== Region Split ======================

func (d *peerMsgHandler) applySplit(entry *pb.Entry, cmdRequest *raft_cmdpb.RaftCmdRequest, kvWB *engine_util.WriteBatch) {
	splitReq := cmdRequest.AdminRequest.GetSplit()
	if splitReq == nil {
		return
	}

	region := d.Region()
	splitKey := splitReq.SplitKey

	// Validate epoch
	if err := util.CheckRegionEpoch(cmdRequest, region, true); err != nil {
		log.Infof("%s split epoch check failed: %v", d.Tag, err)
		d.checkValidAndCallback(&raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
		}, entry)
		return
	}

	// Validate split key is within region range and not equal to start key.
	if err := util.CheckKeyInRegionExclusive(splitKey, region); err != nil {
		log.Infof("%s split key %v not in region: %v", d.Tag, splitKey, err)
		d.checkValidAndCallback(&raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
		}, entry)
		return
	}

	// Validate peer count match
	if len(splitReq.NewPeerIds) != len(region.Peers) {
		log.Infof("%s split peer count mismatch: %d vs %d", d.Tag, len(splitReq.NewPeerIds), len(region.Peers))
		return
	}

	// Create new region peers
	newPeers := make([]*metapb.Peer, 0, len(region.Peers))
	for i, peer := range region.Peers {
		newPeers = append(newPeers, &metapb.Peer{
			Id:      splitReq.NewPeerIds[i],
			StoreId: peer.StoreId,
		})
	}

	// New region takes [splitKey, oldEndKey)
	newVersion := region.RegionEpoch.Version + 1
	newRegion := &metapb.Region{
		Id:       splitReq.NewRegionId,
		StartKey: util.SafeCopy(splitKey),
		EndKey:   util.SafeCopy(region.EndKey),
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: region.RegionEpoch.ConfVer,
			Version: newVersion,
		},
		Peers: newPeers,
	}

	// Old region becomes [oldStartKey, splitKey)
	region.EndKey = util.SafeCopy(splitKey)
	region.RegionEpoch.Version = newVersion

	// Persist both regions
	meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
	meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
	kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
	kvWB.Reset()

	// Update store metadata and collect pending votes for this region.
	var existedPeer *peer
	var pendingVotes []*rspb.RaftMessage
	d.ctx.storeMeta.Lock()
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
	if ps := d.ctx.router.get(newRegion.Id); ps != nil {
		existedPeer = ps.peer
		d.ctx.storeMeta.setRegion(newRegion, existedPeer)
	} else {
		d.ctx.storeMeta.regions[newRegion.Id] = newRegion
	}
	if len(d.ctx.storeMeta.pendingVotes) > 0 {
		remainingVotes := d.ctx.storeMeta.pendingVotes[:0]
		for _, vote := range d.ctx.storeMeta.pendingVotes {
			if vote.GetRegionId() == newRegion.Id {
				pendingVotes = append(pendingVotes, vote)
				continue
			}
			remainingVotes = append(remainingVotes, vote)
		}
		d.ctx.storeMeta.pendingVotes = remainingVotes
	}
	d.ctx.storeMeta.Unlock()

	// Create new peer for the split region on this store if needed.
	newPeer := existedPeer
	if newPeer == nil {
		createdPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
		if err != nil {
			log.Errorf("%s failed to create peer for split region: %v", d.Tag, err)
			return
		}
		newPeer = createdPeer

		d.ctx.router.register(newPeer)
		_ = d.ctx.router.send(newRegion.Id, message.Msg{Type: message.MsgTypeStart})
	}
	// Let the new peer try to campaign if parent is leader.
	newPeer.MaybeCampaign(d.IsLeader())
	for _, vote := range pendingVotes {
		_ = d.ctx.router.send(newRegion.Id, message.Msg{Type: message.MsgTypeRaftMessage, Data: vote})
	}

	// Notify scheduler for both old and new regions.
	d.notifyHeartbeatScheduler(region, d.peer)
	d.notifyHeartbeatScheduler(newRegion, newPeer)

	// Callback
	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType: raft_cmdpb.AdminCmdType_Split,
			Split: &raft_cmdpb.SplitResponse{
				Regions: []*metapb.Region{region, newRegion},
			},
		},
	}
	d.checkValidAndCallback(cmdResp, entry)
}

// ====================== KV Request Application ======================

func (d *peerMsgHandler) applyGetReq(entry *pb.Entry, req *raft_cmdpb.Request) {
	value, err := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
	if err != nil {
		value = nil
	}

	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		Responses: []*raft_cmdpb.Response{{
			CmdType: raft_cmdpb.CmdType_Get,
			Get:     &raft_cmdpb.GetResponse{Value: value},
		}},
	}
	d.checkValidAndCallback(cmdResp, entry)
}

func (d *peerMsgHandler) applyPutReq(entry *pb.Entry, req *raft_cmdpb.Request, kvWB *engine_util.WriteBatch) {
	kvWB.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
	kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
	kvWB.Reset()

	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		Responses: []*raft_cmdpb.Response{{
			CmdType: raft_cmdpb.CmdType_Put,
			Put:     &raft_cmdpb.PutResponse{},
		}},
	}
	d.checkValidAndCallback(cmdResp, entry)
}

func (d *peerMsgHandler) applyDeleteReq(entry *pb.Entry, req *raft_cmdpb.Request, kvWB *engine_util.WriteBatch) {
	kvWB.DeleteCF(req.Delete.Cf, req.Delete.Key)
	kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
	kvWB.Reset()

	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		Responses: []*raft_cmdpb.Response{{
			CmdType: raft_cmdpb.CmdType_Delete,
			Delete:  &raft_cmdpb.DeleteResponse{},
		}},
	}
	d.checkValidAndCallback(cmdResp, entry)
}

func (d *peerMsgHandler) applySnapReq(entry *pb.Entry, cmdRequest *raft_cmdpb.RaftCmdRequest) {
	// Check epoch for snap request to avoid returning stale region info after split
	if cmdRequest.Header != nil && cmdRequest.Header.RegionEpoch != nil {
		if err := util.CheckRegionEpoch(cmdRequest, d.Region(), true); err != nil {
			d.checkValidAndCallback(&raft_cmdpb.RaftCmdResponse{
				Header: &raft_cmdpb.RaftResponseHeader{},
			}, entry)
			return
		}
	}

	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		Responses: []*raft_cmdpb.Response{{
			CmdType: raft_cmdpb.CmdType_Snap,
			Snap:    &raft_cmdpb.SnapResponse{Region: d.Region()},
		}},
	}
	d.checkValidAndCallback(cmdResp, entry, d.peerStorage.Engines.Kv.NewTransaction(false))
}

// ====================== Proposal Matching & Callback ======================

func (d *peerMsgHandler) checkValidAndCallback(resp *raft_cmdpb.RaftCmdResponse, entry *pb.Entry, snapTxn ...*badger.Txn) {
	for len(d.proposals) > 0 {
		proposal := d.proposals[0]

		if entry.Term < proposal.term {
			return
		} else if entry.Term > proposal.term {
			NotifyStaleReq(proposal.term, proposal.cb)
			d.proposals = d.proposals[1:]
			continue
		} else if entry.Index < proposal.index {
			return
		} else if entry.Index > proposal.index {
			NotifyStaleReq(proposal.term, proposal.cb)
			d.proposals = d.proposals[1:]
			continue
		} else {
			if snapTxn != nil {
				proposal.cb.Txn = snapTxn[0]
			}
			proposal.cb.Done(resp)
			d.proposals = d.proposals[1:]
			return
		}
	}
}

// ====================== Message Handling ======================

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

// ====================== Proposal ======================

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		if cb != nil {
			cb.Done(ErrResp(err))
		}
		return
	}

	if msg.AdminRequest != nil {
		d.proposeAdminRequest(msg, cb)
		return
	}

	// For normal requests: validate all keys, then propose once
	for _, req := range msg.Requests {
		var key []byte
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			key = req.Get.Key
		case raft_cmdpb.CmdType_Put:
			key = req.Put.Key
		case raft_cmdpb.CmdType_Delete:
			key = req.Delete.Key
		case raft_cmdpb.CmdType_Snap:
			// No key check needed for Snap
			continue
		}
		if key != nil {
			if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
				if cb != nil {
					cb.Done(ErrResp(err))
				}
				return
			}
		}
	}

	// Propose the entire message once
	d.proposeToRaftGroup(msg, cb)
}

func (d *peerMsgHandler) proposeAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	adminReq := msg.GetAdminRequest()
	switch adminReq.CmdType {
	case raft_cmdpb.AdminCmdType_ChangePeer:
		d.handleChangePeerReq(msg, adminReq, cb)
	case raft_cmdpb.AdminCmdType_CompactLog:
		d.proposeToRaftGroup(msg, cb)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		d.handleTransferLeaderReq(adminReq, cb)
	case raft_cmdpb.AdminCmdType_Split:
		d.handleSplitReq(msg, cb)
	default:
		if cb != nil {
			cb.Done(ErrResp(fmt.Errorf("invalid admin cmd type")))
		}
	}
}

func (d *peerMsgHandler) proposeToRaftGroup(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	data, err := msg.Marshal()
	if err != nil {
		log.Errorf("failed to marshal raft cmd request: %v", err)
		if cb != nil {
			cb.Done(ErrResp(err))
		}
		return
	}

	var p *proposal
	if cb != nil {
		p = &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
	}

	if err = d.RaftGroup.Propose(data); err != nil {
		log.Errorf("propose failed: %v", err)
		if cb != nil {
			cb.Done(ErrResp(err))
		}
		return
	}
	if p != nil {
		d.proposals = append(d.proposals, p)
	}
}

func (d *peerMsgHandler) handleChangePeerReq(msg *raft_cmdpb.RaftCmdRequest, adminReq *raft_cmdpb.AdminRequest, cb *message.Callback) {
	data, err := msg.Marshal()
	if err != nil {
		log.Errorf("failed to marshal raft cmd request: %v", err)
		cb.Done(ErrResp(err))
		return
	}
	if adminReq.ChangePeer == nil {
		err = fmt.Errorf("invalid ChangePeer request: ChangePeer is nil")
		cb.Done(ErrResp(err))
		return
	}

	cc := pb.ConfChange{
		ChangeType: adminReq.ChangePeer.ChangeType,
		NodeId:     adminReq.ChangePeer.Peer.Id,
		Context:    data,
	}

	proposal := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}

	if err = d.RaftGroup.ProposeConfChange(cc); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	d.proposals = append(d.proposals, proposal)
}

func (d *peerMsgHandler) handleTransferLeaderReq(adminReq *raft_cmdpb.AdminRequest, cb *message.Callback) {
	d.RaftGroup.TransferLeader(adminReq.TransferLeader.Peer.Id)
	cb.Done(&raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
			TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
		},
	})
}

func (d *peerMsgHandler) handleSplitReq(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	// Validate split key is in region
	splitReq := msg.AdminRequest.GetSplit()
	if splitReq == nil {
		if cb != nil {
			cb.Done(ErrResp(fmt.Errorf("split request is nil")))
		}
		return
	}
	if err := util.CheckKeyInRegion(splitReq.SplitKey, d.Region()); err != nil {
		if cb != nil {
			cb.Done(ErrResp(err))
		}
		return
	}

	d.proposeToRaftGroup(msg, cb)
}

// ====================== Scheduler & Heartbeat ======================

func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}

// ====================== Tick Handlers ======================

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}
	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

// ====================== Raft Message Handling ======================

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// ====================== Peer Lifecycle ======================

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		log.Warnf("%s region range item already removed when destroying peer", d.Tag)
	}
	if _, ok := meta.regions[regionID]; ok {
		delete(meta.regions, regionID)
	} else {
		log.Warnf("%s region meta already removed when destroying peer", d.Tag)
	}
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

// ====================== Split Region Prepare ======================

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

// ====================== Misc ======================

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

// ====================== Request Builders ======================

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
