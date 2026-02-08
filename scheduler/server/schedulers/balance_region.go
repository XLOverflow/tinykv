// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).

	// 1. Select all suitable stores (up and not recently down)
	stores := make([]*core.StoreInfo, 0)
	for _, store := range cluster.GetStores() {
		if store.IsUp() && store.DownTime() < cluster.GetMaxStoreDownTime() {
			stores = append(stores, store)
		}
	}
	if len(stores) == 0 {
		return nil
	}

	// 2. Sort stores by region size (descending) to find source
	sort.Slice(stores, func(i, j int) bool {
		return stores[i].GetRegionSize() > stores[j].GetRegionSize()
	})

	// 3. Try each store as source (from highest region size)
	var region *core.RegionInfo
	var sourceStore *core.StoreInfo

	// regionFilter only selects regions with enough replicas
	regionFilter := func(r *core.RegionInfo) bool {
		return len(r.GetPeers()) >= cluster.GetMaxReplicas()
	}

	for _, store := range stores {
		var r *core.RegionInfo
		// Try pending region first, then follower, then leader
		cluster.GetPendingRegionsWithLock(store.GetID(), func(rc core.RegionsContainer) {
			r = rc.RandomRegion(nil, nil)
		})
		if r != nil && regionFilter(r) {
			region = r
			sourceStore = store
			break
		}
		cluster.GetFollowersWithLock(store.GetID(), func(rc core.RegionsContainer) {
			r = rc.RandomRegion(nil, nil)
		})
		if r != nil && regionFilter(r) {
			region = r
			sourceStore = store
			break
		}
		cluster.GetLeadersWithLock(store.GetID(), func(rc core.RegionsContainer) {
			r = rc.RandomRegion(nil, nil)
		})
		if r != nil && regionFilter(r) {
			region = r
			sourceStore = store
			break
		}
	}

	if region == nil || sourceStore == nil {
		return nil
	}

	// 4. Find the target store with smallest region size that doesn't already have a peer
	storeIDs := make(map[uint64]struct{})
	for _, peer := range region.GetPeers() {
		storeIDs[peer.GetStoreId()] = struct{}{}
	}

	var targetStore *core.StoreInfo
	for i := len(stores) - 1; i >= 0; i-- {
		store := stores[i]
		if _, ok := storeIDs[store.GetID()]; !ok {
			targetStore = store
			break
		}
	}

	if targetStore == nil {
		return nil
	}

	// 5. Check if the difference is large enough to justify moving
	// Only move if source has significantly more regions than target
	if sourceStore.GetRegionSize()-targetStore.GetRegionSize() < 2*region.GetApproximateSize() {
		return nil
	}

	// 6. Create the move peer operator
	newPeer, err := cluster.AllocPeer(targetStore.GetID())
	if err != nil {
		return nil
	}

	op, err := operator.CreateMovePeerOperator("balance-region", cluster, region, operator.OpBalance, sourceStore.GetID(), targetStore.GetID(), newPeer.GetId())
	if err != nil {
		return nil
	}
	return op
}
