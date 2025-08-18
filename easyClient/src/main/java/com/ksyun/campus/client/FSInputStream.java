package com.ksyun.campus.client;

import dfs_project.Metaserver;
import com.ksyun.campus.client.client.MinFSClient;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class FSInputStream extends InputStream {

    private final MinFSClient client;
    private final String path;
    private final long fileSize;
    private long position;
    private byte[] currentBlock;
    private int blockPosition;
    private long currentBlockId;

    public FSInputStream(MinFSClient client, String path, long fileSize) {
        this.client = client;
        this.path = path;
        this.fileSize = fileSize;
        this.position = 0;
        this.currentBlock = null;
        this.blockPosition = 0;
        this.currentBlockId = -1;
    }

    @Override
    public int read() throws IOException {
        if (position >= fileSize) {
            return -1;
        }

        if (currentBlock == null || blockPosition >= currentBlock.length) {
            if (!loadNextBlock()) {
                return -1;
            }
        }

        int value = currentBlock[blockPosition] & 0xFF;
        blockPosition++;
        position++;
        return value;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (position >= fileSize) {
            return -1;
        }

        int totalRead = 0;
        while (len > 0 && position < fileSize) {
            if (currentBlock == null || blockPosition >= currentBlock.length) {
                if (!loadNextBlock()) {
                    break;
                }
            }

            int remainingInBlock = currentBlock.length - blockPosition;
            int bytesToRead = Math.min(remainingInBlock, len);
            System.arraycopy(currentBlock, blockPosition, b, off, bytesToRead);

            blockPosition += bytesToRead;
            position += bytesToRead;
            off += bytesToRead;
            len -= bytesToRead;
            totalRead += bytesToRead;
        }

        return totalRead == 0 && position >= fileSize ? -1 : totalRead;
    }

    private boolean loadNextBlock() throws IOException {
        // 计算当前位置需要的块索引
        long currentBlockIndex = position / MinFSClient.DEFAULT_BLOCK_SIZE;
        
        // 获取块位置信息
        Metaserver.GetBlockLocationsResponse response = client.getBlockLocations(path, fileSize);
        if (response == null) {
            throw new IOException("Failed to get block locations for file: " + path);
        }

        List<Metaserver.BlockLocations> blocks = response.getBlockLocationsList();
        if (currentBlockIndex >= blocks.size()) {
            return false; // 已经到文件末尾
        }

        Metaserver.BlockLocations block = blocks.get((int) currentBlockIndex);
        String dataServer = block.getLocations(0); // Use first replica
        long newBlockId = block.getBlockId();
        
        // 只有当块ID不同时才重新加载
        if (currentBlockId != newBlockId) {
            currentBlockId = newBlockId;
            currentBlock = client.readBlockFromDataServer(dataServer, currentBlockId);
            if (currentBlock == null) {
                throw new IOException("Failed to read block " + currentBlockId + " from " + dataServer);
            }
        }

        // 计算在当前块内的位置
        blockPosition = (int) (position % MinFSClient.DEFAULT_BLOCK_SIZE);
        return true;
    }

    @Override
    public void close() throws IOException {
        currentBlock = null;
    }

    @Override
    public long skip(long n) throws IOException {
        long remaining = fileSize - position;
        if (n > remaining) {
            n = remaining;
        }
        position += n;
        blockPosition += n;
        if (currentBlock != null && blockPosition >= currentBlock.length) {
            currentBlock = null;
        }
        return n;
    }

    @Override
    public int available() throws IOException {
        if (currentBlock == null) {
            return 0;
        }
        return currentBlock.length - blockPosition;
    }
}