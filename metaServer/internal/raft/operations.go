package raft

// CreateNodeOperation 创建节点操作
type CreateNodeOperation struct {
	Path      string `json:"path"`
	NodeType  string `json:"node_type"` // "FILE" or "DIRECTORY"
	InodeID   uint64 `json:"inode_id"`
	ParentIno uint64 `json:"parent_ino"`
}

// DeleteNodeOperation 删除节点操作
type DeleteNodeOperation struct {
	Path      string `json:"path"`
	Recursive bool   `json:"recursive"`
}

// FinalizeWriteOperation 完成写入操作
type FinalizeWriteOperation struct {
	Path   string                    `json:"path"`
	Inode  uint64                    `json:"inode"`
	Size   uint64                    `json:"size"`
	MD5    string                    `json:"md5"`
	Blocks []*BlockLocationOperation `json:"blocks"`
}

// BlockLocationOperation 块位置信息
type BlockLocationOperation struct {
	BlockID   uint64   `json:"block_id"`
	Offset    int64    `json:"offset"`
	Size      int64    `json:"size"`
	DataNodes []string `json:"data_nodes"`
	Checksum  string   `json:"checksum"`
}

// SetBlockMappingOperation 设置块映射操作
type SetBlockMappingOperation struct {
	InodeID    uint64                    `json:"inode_id"`
	BlockIndex uint64                    `json:"block_index"`
	BlockID    uint64                    `json:"block_id"`
	Locations  []string                  `json:"locations"`
}

// UpdateBlockLocationOperation 更新块位置操作
type UpdateBlockLocationOperation struct {
	BlockID uint64 `json:"block_id"`
	OldAddr string `json:"old_addr"`
	NewAddr string `json:"new_addr"`
}
