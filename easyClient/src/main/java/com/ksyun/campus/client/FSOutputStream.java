package com.ksyun.campus.client;

import dfs_project.Metaserver;
import com.ksyun.campus.client.client.MinFSClient;
import java.security.MessageDigest;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class FSOutputStream extends OutputStream {

    private final MinFSClient client;
    private final String path;
    private final ByteArrayBuffer buffer;
    private long inode;
    private boolean finalized;

    public FSOutputStream(MinFSClient client, String path) {
        this.client = client;
        this.path = path;
        this.buffer = new ByteArrayBuffer(MinFSClient.DEFAULT_BLOCK_SIZE);
        this.inode = -1;
        this.finalized = false;
    }

    @Override
    public void write(int b) throws IOException {
        if (finalized) {
            throw new IOException("Stream already closed");
        }
        buffer.write((byte) b);
        // 不要立即flush，等到close时一次性写入
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (finalized) {
            throw new IOException("Stream already closed");
        }

        // 直接写入缓冲区，不管容量限制，让缓冲区自动扩展
        buffer.write(b, off, len);
        
        // 不要立即写入，等到close时一次性写入
        // 这样符合 create -> write -> close 的三步曲逻辑
    }

    @Override
    public void flush() throws IOException {
        // 不做实际flush，等到close时一次性写入
    }

    @Override
    public void close() throws IOException {
        if (finalized) {
            return;
        }

        try {
            // 采用Golang客户端策略：一次性写入所有数据
            writeFileAtOnce();
            finalized = true;
            // 打印写入成功信息
            System.out.println("[SDK] 写入成功: " + path + " (" + buffer.totalSize() + " 字节)");
        } catch (Exception e) {
            System.err.println("[SDK] 写入失败: " + path + " - " + e.getMessage());
            throw e;
        } finally {
            buffer.clear();
        }
    }

    /**
     * 采用Golang客户端的成功策略：一次性获取所有块位置，然后分割数据写入
     */
    private void writeFileAtOnce() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }

        byte[] allData = buffer.getData();
        long totalSize = buffer.totalSize();

        try {
            // 1. 一次性获取所有块位置信息（传递完整文件大小，类似Golang客户端）
            Metaserver.GetBlockLocationsResponse response = client.getBlockLocations(path, totalSize);
            if (response == null) {
                throw new IOException("无法获取块位置信息: " + path);
            }

            List<Metaserver.BlockLocations> blocks = response.getBlockLocationsList();
            if (blocks.isEmpty()) {
                throw new IOException("没有分配到数据块: " + path);
            }

            this.inode = response.getInode();

            // 2. 分割数据并按块写入（类似Golang客户端的逻辑）
            int blockSize = MinFSClient.DEFAULT_BLOCK_SIZE; // 4MB
            int dataOffset = 0;
            
            for (int i = 0; i < blocks.size() && dataOffset < allData.length; i++) {
                Metaserver.BlockLocations block = blocks.get(i);
                
                // 计算这个块的数据范围
                int startOffset = dataOffset;
                int endOffset = Math.min(startOffset + blockSize, allData.length);
                
                // 获取这个块的数据片段
                byte[] blockData = new byte[endOffset - startOffset];
                System.arraycopy(allData, startOffset, blockData, 0, endOffset - startOffset);
                
                // 写入到第一个DataServer，让它处理副本复制
                String dataServerAddr = block.getLocations(0);
                List<String> replicaLocations = new ArrayList<>(block.getLocationsList());
                
                boolean success = client.writeBlockToDataServer(
                    dataServerAddr, 
                    block.getBlockId(), 
                    blockData, 
                    replicaLocations
                );
                
                if (!success) {
                    throw new IOException(String.format("写入块 %d 到 %s 失败", 
                        block.getBlockId(), dataServerAddr));
                }
                
                dataOffset = endOffset;
            }

            // 3. 计算MD5哈希并完成写入
            String md5Hash = buffer.getMD5();
            boolean finalized = client.finalizeWrite(path, this.inode, totalSize, md5Hash);
            
            if (!finalized) {
                throw new IOException("完成文件写入失败: " + path);
            }
                
        } catch (Exception e) {
            throw new IOException("写入文件失败: " + e.getMessage(), e);
        }
    }

    private static class ByteArrayBuffer {
        private byte[] buffer;
        private int position;
        private long totalSize;
        private final MessageDigest md5Digest;

        public ByteArrayBuffer(int capacity) {
            this.buffer = new byte[capacity];
            this.position = 0;
            this.totalSize = 0;
            try {
                this.md5Digest = MessageDigest.getInstance("MD5");
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize MD5 digest", e);
            }
        }

        public void write(byte b) {
            ensureCapacity(1);
            buffer[position++] = b;
            md5Digest.update(b);
            totalSize++;
        }

        public void write(byte[] b, int off, int len) {
            ensureCapacity(len);
            System.arraycopy(b, off, buffer, position, len);
            md5Digest.update(b, off, len);
            position += len;
            totalSize += len;
        }

        private void ensureCapacity(int additionalCapacity) {
            int requiredCapacity = position + additionalCapacity;
            if (requiredCapacity > buffer.length) {
                // 扩容到所需大小的1.5倍，最小为原大小的2倍
                int newCapacity = Math.max(requiredCapacity, buffer.length * 2);
                byte[] newBuffer = new byte[newCapacity];
                System.arraycopy(buffer, 0, newBuffer, 0, position);
                buffer = newBuffer;
            }
        }

        public byte[] getData() {
            byte[] result = new byte[position];
            System.arraycopy(buffer, 0, result, 0, position);
            return result;
        }

        public int size() {
            return position;
        }

        public long totalSize() {
            return totalSize;
        }

        public boolean isEmpty() {
            return position == 0;
        }

        public boolean isFull() {
            return position == buffer.length;
        }

        public int remaining() {
            return buffer.length - position;
        }

        public void clear() {
            position = 0;
        }

        public String getMD5() {
            byte[] hash = md5Digest.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        }
    }
}