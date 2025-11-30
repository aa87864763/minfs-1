package raft

import (
	"encoding/json"
	"time"
)

// OperationType 操作类型枚举
type OperationType uint8

const (
	OpCreateNode OperationType = iota + 1
	OpDeleteNode
	OpFinalizeWrite
	OpSetBlockMapping
	OpUpdateBlockLocation
)

// String 返回操作类型的字符串表示
func (op OperationType) String() string {
	switch op {
	case OpCreateNode:
		return "CREATE_NODE"
	case OpDeleteNode:
		return "DELETE_NODE"
	case OpFinalizeWrite:
		return "FINALIZE_WRITE"
	case OpSetBlockMapping:
		return "SET_BLOCK_MAPPING"
	case OpUpdateBlockLocation:
		return "UPDATE_BLOCK_LOCATION"
	default:
		return "UNKNOWN"
	}
}

// RaftLogEntry 统一的日志条目格式
type RaftLogEntry struct {
	Type      OperationType   `json:"type"`
	Data      json.RawMessage `json:"data"`
	Timestamp int64           `json:"timestamp"`
}

// NewRaftLogEntry 创建日志条目
func NewRaftLogEntry(opType OperationType, data interface{}) (*RaftLogEntry, error) {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	
	return &RaftLogEntry{
		Type:      opType,
		Data:      dataBytes,
		Timestamp: time.Now().UnixMilli(),
	}, nil
}

// Marshal 序列化日志条目
func (e *RaftLogEntry) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

// UnmarshalRaftLogEntry 反序列化日志条目
func UnmarshalRaftLogEntry(data []byte) (*RaftLogEntry, error) {
	var entry RaftLogEntry
	err := json.Unmarshal(data, &entry)
	return &entry, err
}
