package raft

import (
	"log"
	
	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
)

// MetadataSnapshot 实现 raft.FSMSnapshot 接口
type MetadataSnapshot struct {
	db *badger.DB
}

// Persist 持久化快照到 sink
// Raft 会调用此方法将快照写入持久化存储
func (s *MetadataSnapshot) Persist(sink raft.SnapshotSink) error {
	log.Printf("[Snapshot] Persisting snapshot to sink")
	
	defer func() {
		if err := sink.Close(); err != nil {
			log.Printf("[Snapshot] Failed to close sink: %v", err)
		}
	}()
	
	// 使用 BadgerDB 的 Backup 功能创建快照
	// since=0 表示全量备份
	since := uint64(0)
	_, err := s.db.Backup(sink, since)
	if err != nil {
		log.Printf("[Snapshot] Backup failed: %v", err)
		sink.Cancel()
		return err
	}
	
	log.Printf("[Snapshot] Snapshot persisted successfully")
	return nil
}

// Release 释放快照资源
func (s *MetadataSnapshot) Release() {
	log.Printf("[Snapshot] Releasing snapshot resources")
	// BadgerDB 的 Backup 不需要额外清理
}
