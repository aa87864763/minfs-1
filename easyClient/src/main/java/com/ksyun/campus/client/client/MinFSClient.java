package com.ksyun.campus.client.client;

import com.google.protobuf.ByteString;
import dfs_project.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import com.ksyun.campus.client.discovery.EtcdServiceDiscovery;

import java.io.*;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * MinFS分布式文件系统客户端
 * 提供完整的文件系统操作接口
 * 支持 etcd 服务发现
 */
public class MinFSClient implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(MinFSClient.class.getName());

    private ManagedChannel channel;
    private MetaServerServiceGrpc.MetaServerServiceBlockingStub blockingStub;
    private MetaServerServiceGrpc.MetaServerServiceStub asyncStub;

    private final String metaServerAddress;
    private final int port;
    private final EtcdServiceDiscovery serviceDiscovery;
    private final boolean useEtcd;

    // 数据块大小 (4MB)
    public static final int DEFAULT_BLOCK_SIZE = 4 * 1024 * 1024;

    /**
     * 构造函数 - 使用固定地址
     * @param metaServerAddress 元数据服务器地址
     * @param port 端口号
     */
    public MinFSClient(String metaServerAddress, int port) {
        this.metaServerAddress = metaServerAddress;
        this.port = port;
        this.serviceDiscovery = null;
        this.useEtcd = false;

        // 创建gRPC通道
        this.channel = ManagedChannelBuilder.forAddress(metaServerAddress, port)
                .usePlaintext()
                .build();

        this.blockingStub = MetaServerServiceGrpc.newBlockingStub(channel);
        this.asyncStub = MetaServerServiceGrpc.newStub(channel);
    }

    /**
     * 构造函数 - 使用 etcd 服务发现
     * @param etcdEndpoints etcd 服务地址列表，如 "http://localhost:2379"
     * @param basePath 服务注册的基础路径
     */
    public MinFSClient(String etcdEndpoints, String basePath, boolean useEtcd) {
        this.metaServerAddress = null;
        this.port = 0;
        this.serviceDiscovery = new EtcdServiceDiscovery(etcdEndpoints, basePath);
        this.useEtcd = useEtcd;

        // 等待服务发现完成后再初始化连接
        initializeConnectionWithRetry();
    }

    /**
     * 等待服务发现完成后初始化连接
     */
    private void initializeConnectionWithRetry() {
        if (!useEtcd || serviceDiscovery == null) {
            return;
        }

        // 最多等待10秒，每500ms检查一次
        int maxRetries = 20;
        int retryCount = 0;
        
        while (retryCount < maxRetries) {
            try {
                // 尝试获取可用的 MetaServer 地址
                String availableServer = serviceDiscovery.getAvailableMetaServer();
                if (availableServer != null) {
                    // 成功获取到服务地址，进行连接初始化
                    initializeConnection(availableServer);
                    return;
                }
                
                // 如果没有获取到服务，等待500ms后重试
                Thread.sleep(500);
                retryCount++;
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("初始化连接被中断", e);
            } catch (Exception e) {
                // 其他异常，继续重试
                retryCount++;
                if (retryCount >= maxRetries) {
                    throw new RuntimeException("初始化连接失败: " + e.getMessage(), e);
                }
            }
        }
        
        throw new RuntimeException("初始化连接超时: 在10秒内未能发现可用的 MetaServer");
    }

    /**
     * 初始化连接
     */
    private void initializeConnection(String availableServer) {
        try {

            // 解析地址和端口
            String[] parts = availableServer.split(":");
            if (parts.length != 2) {
                throw new RuntimeException("无效的服务地址格式: " + availableServer);
            }

            String address = parts[0];
            int port = Integer.parseInt(parts[1]);

            logger.info("从 etcd 获取到 MetaServer 地址: " + address + ":" + port);

            // 创建gRPC通道
            this.channel = ManagedChannelBuilder.forAddress(address, port)
                    .usePlaintext()
                    .build();

            this.blockingStub = MetaServerServiceGrpc.newBlockingStub(channel);
            this.asyncStub = MetaServerServiceGrpc.newStub(channel);

        } catch (Exception e) {
            logger.severe("初始化连接失败: " + e.getMessage());
            throw new RuntimeException("初始化连接失败", e);
        }
    }

    /**
     * 重新连接（当服务地址变化时）
     */
    private void reconnect() {
        if (!useEtcd || serviceDiscovery == null) {
            return;
        }

        try {
            // 关闭旧连接
            if (channel != null && !channel.isShutdown()) {
                channel.shutdown();
            }

            // 重新初始化连接
            initializeConnectionWithRetry();

        } catch (Exception e) {
            logger.warning("重新连接失败: " + e.getMessage());
        }
    }

    /**
     * 功能接口：支持抛出异常的操作
     */
    @FunctionalInterface
    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    /**
     * 功能接口：支持抛出异常的无返回值操作
     */
    @FunctionalInterface  
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /**
     * 故障转移中间件：自动处理Leader切换和重试
     * @param operation 要执行的操作
     * @param defaultValue 失败时的默认返回值
     * @param operationName 操作名称（用于日志）
     * @return 操作结果
     */
    private <T> T executeWithFailover(ThrowingSupplier<T> operation, T defaultValue, String operationName) {
        return executeWithFailoverInternal(operation, defaultValue, operationName, false);
    }

    /**
     * 故障转移中间件：无返回值版本
     * @param operation 要执行的操作
     * @param operationName 操作名称（用于日志）
     * @return 是否成功
     */
    private boolean executeWithFailover(ThrowingRunnable operation, String operationName) {
        return executeWithFailoverInternal(() -> {
            operation.run();
            return true;
        }, false, operationName, false);
    }

    /**
     * 核心故障转移逻辑
     */
    private <T> T executeWithFailoverInternal(ThrowingSupplier<T> operation, T defaultValue, String operationName, boolean isRetry) {
        try {
            T result = operation.get();
            if (isRetry) {
                logger.info(operationName + " 重试成功");
            }
            return result;
        } catch (StatusRuntimeException e) {
            // 检查是否为可重试的连接错误
            boolean shouldRetry = useEtcd && !isRetry && isRetryableError(e);
            
            if (shouldRetry) {
                logger.info(operationName + " 检测到Leader连接失败，尝试重新发现Leader: " + e.getStatus().getDescription());
                
                try {
                    reconnect();
                    logger.info(operationName + " 重新连接成功，重试操作...");
                    // 递归调用，但标记为重试状态避免无限循环
                    return executeWithFailoverInternal(operation, defaultValue, operationName, true);
                } catch (Exception retryException) {
                    logger.severe(operationName + " 重试失败: " + retryException.getMessage());
                    return defaultValue;
                }
            } else {
                logger.severe(operationName + " 失败: " + e.getStatus().getDescription());
                return defaultValue;
            }
        } catch (Exception e) {
            logger.severe(operationName + " 异常: " + e.getMessage());
            return defaultValue;
        }
    }

    /**
     * 判断是否为可重试的错误
     */
    private boolean isRetryableError(StatusRuntimeException e) {
        Status.Code code = e.getStatus().getCode();
        return code == Status.UNAVAILABLE.getCode() || 
               code == Status.DEADLINE_EXCEEDED.getCode() ||
               code == Status.ABORTED.getCode() ||
               code == Status.INTERNAL.getCode();
    }

    /**
     * 创建文件
     * @param path 文件路径
     * @return 操作结果
     */
    public boolean createFile(String path) {
        return executeWithFailover(() -> {
            Metaserver.CreateNodeRequest request = Metaserver.CreateNodeRequest.newBuilder()
                    .setPath(path)
                    .setType(Metaserver.FileType.File)
                    .build();

            Metaserver.SimpleResponse response = blockingStub.createNode(request);
            boolean success = response.getSuccess();

            if (success) {
                logger.info("文件创建成功: " + path);
            } else {
                logger.warning("文件创建失败: " + path);
            }

            return success;
        }, false, "创建文件[" + path + "]");
    }

    /**
     * 创建目录
     * @param path 目录路径
     * @return 操作结果
     */
    public boolean createDirectory(String path) {
        return executeWithFailover(() -> {
            Metaserver.CreateNodeRequest request = Metaserver.CreateNodeRequest.newBuilder()
                    .setPath(path)
                    .setType(Metaserver.FileType.Directory)
                    .build();

            Metaserver.SimpleResponse response = blockingStub.createNode(request);
            boolean success = response.getSuccess();

            if (success) {
                logger.info("目录创建成功: " + path);
            } else {
                logger.warning("目录创建失败: " + path);
            }

            return success;
        }, false, "创建目录[" + path + "]");
    }

    /**
     * 获取文件或目录的属性信息
     * @param path 路径
     * @return 节点信息，如果失败返回null
     */
    public Metaserver.StatInfo getStatus(String path) {
        return executeWithFailover(() -> {
            Metaserver.GetNodeInfoRequest request =
                    Metaserver.GetNodeInfoRequest.newBuilder()
                            .setPath(path)
                            .build();

            Metaserver.GetNodeInfoResponse response = blockingStub.getNodeInfo(request);
            Metaserver.StatInfo statInfo = response.getStatInfo();

            if (statInfo != null) {
                logger.info("获取状态成功: " + path +
                        ", 类型: " + statInfo.getType() +
                        ", 大小: " + statInfo.getSize() +
                        ", mtime: " + statInfo.getMtime());
            }
            return statInfo;
        }, null, "获取状态[" + path + "]");
    }


    /**
     * 列出目录下的所有条目
     * @param path 目录路径
     * @return 目录下的节点列表，如果失败返回null
     */
    public List<Metaserver.StatInfo> listStatus(String path) {
        return executeWithFailover(() -> {
            Metaserver.ListDirectoryRequest request =
                    Metaserver.ListDirectoryRequest.newBuilder()
                            .setPath(path)
                            .build();

            Metaserver.ListDirectoryResponse response = blockingStub.listDirectory(request);
            List<Metaserver.StatInfo> nodes = response.getNodesList();

            logger.info("列出目录成功: " + path + ", 条目数: " + nodes.size());
            return nodes;
        }, null, "列出目录[" + path + "]");
    }

    /**
     * 删除文件或目录
     * @param path 路径
     * @param recursive 是否递归删除（用于目录）
     * @return 操作结果
     */
    public boolean delete(String path, boolean recursive) {
        try {
            Metaserver.DeleteNodeRequest request = Metaserver.DeleteNodeRequest.newBuilder()
                    .setPath(path)
                    .setRecursive(recursive)
                    .build();

            Metaserver.SimpleResponse response = blockingStub.deleteNode(request);
            boolean success = response.getSuccess();

            if (success) {
                logger.info("删除成功: " + path + (recursive ? " (递归)" : ""));
            } else {
                logger.warning("删除失败: " + path);
            }

            return success;
        } catch (StatusRuntimeException e) {
            logger.severe("删除时发生gRPC错误: " + e.getStatus().getDescription());
            return false;
        } catch (Exception e) {
            logger.severe("删除时发生异常: " + e.getMessage());
            return false;
        }
    }

    /**
     * 删除文件
     * @param path 文件路径
     * @return 操作结果
     */
    public boolean deleteFile(String path) {
        return delete(path, false);
    }

    /**
     * 删除目录（支持递归）
     * @param path 目录路径
     * @return 操作结果
     */
    public boolean deleteDirectory(String path) {
        return delete(path, true);
    }

    /**
     * 获取文件的数据块位置信息（用于读写操作）
     * @param path 文件路径
     * @param size 文件大小（写操作时使用）
     * @return 块位置信息，如果失败返回null
     */
    public Metaserver.GetBlockLocationsResponse getBlockLocations(String path, long size) {
        try {
            Metaserver.GetBlockLocationsRequest request = Metaserver.GetBlockLocationsRequest.newBuilder()
                    .setPath(path)
                    .setSize(size)
                    .build();

            Metaserver.GetBlockLocationsResponse response = blockingStub.getBlockLocations(request);
            logger.info("获取块位置信息成功: " + path + ", 块数量: " + response.getBlockLocationsList().size());
            return response;
        } catch (StatusRuntimeException e) {
            logger.severe("获取块位置信息时发生gRPC错误: " + e.getStatus().getDescription());
            return null;
        } catch (Exception e) {
            logger.severe("获取块位置信息时发生异常: " + e.getMessage());
            return null;
        }
    }

    /**
     * 完成文件写入操作
     * @param path 文件路径
     * @param inode 文件inode
     * @param size 文件大小
     * @param md5 文件MD5值
     * @return 操作结果
     */
    public boolean finalizeWrite(String path, long inode, long size, String md5) {
        try {
            Metaserver.FinalizeWriteRequest request = Metaserver.FinalizeWriteRequest.newBuilder()
                    .setPath(path)
                    .setInode(inode)
                    .setSize(size)
                    .setMd5(md5)
                    .build();

            Metaserver.SimpleResponse response = blockingStub.finalizeWrite(request);
            boolean success = response.getSuccess();

            if (success) {
                logger.info("完成文件写入: " + path + ", 大小: " + size);
            } else {
                logger.warning("完成文件写入失败: " + path + ", 消息: " + response.getMessage());
            }

            return success;
        } catch (StatusRuntimeException e) {
            logger.severe("完成文件写入时发生gRPC错误: " + e.getStatus().getDescription());
            return false;
        } catch (Exception e) {
            logger.severe("完成文件写入时发生异常: " + e.getMessage());
            return false;
        }
    }

    /**
     * 从DataServer读取数据块
     * @param dataServerAddress DataServer地址
     * @param blockId 块ID
     * @return 数据块内容，如果失败返回null
     */
    public byte[] readBlockFromDataServer(String dataServerAddress, long blockId) {
        ManagedChannel dataChannel = null;
        try {
            String[] parts = dataServerAddress.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            dataChannel = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .build();

            DataServerServiceGrpc.DataServerServiceBlockingStub dataStub =
                    DataServerServiceGrpc.newBlockingStub(dataChannel);

            Dataserver.ReadBlockRequest request = Dataserver.ReadBlockRequest.newBuilder()
                    .setBlockId(blockId)
                    .build();

            Iterator<Dataserver.ReadBlockResponse> responses = dataStub.readBlock(request);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            while (responses.hasNext()) {
                Dataserver.ReadBlockResponse response = responses.next();
                outputStream.write(response.getChunkData().toByteArray());
            }

            byte[] data = outputStream.toByteArray();
            logger.info("从DataServer读取数据块成功: " + dataServerAddress + ", 块ID: " + blockId + ", 大小: " + data.length);
            return data;

        } catch (Exception e) {
            logger.severe("从DataServer读取数据块时发生异常: " + e.getMessage());
            return null;
        } finally {
            if (dataChannel != null && !dataChannel.isShutdown()) {
                try {
                    dataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * 向DataServer写入数据块
     * @param dataServerAddress DataServer地址
     * @param blockId 块ID
     * @param data 数据内容
     * @param replicaLocations 副本位置
     * @return 操作结果
     */
    public boolean writeBlockToDataServer(String dataServerAddress, long blockId, byte[] data, List<String> replicaLocations) {
        ManagedChannel dataChannel = null;
        try {
            String[] parts = dataServerAddress.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            dataChannel = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .build();

            DataServerServiceGrpc.DataServerServiceStub dataStub =
                    DataServerServiceGrpc.newStub(dataChannel);

            StreamObserver<Dataserver.WriteBlockRequest> requestObserver =
                    dataStub.writeBlock(new StreamObserver<Dataserver.WriteBlockResponse>() {
                        @Override
                        public void onNext(Dataserver.WriteBlockResponse response) {
                            logger.info("写入数据块响应: " + response.getSuccess());
                        }

                        @Override
                        public void onError(Throwable t) {
                            logger.severe("写入数据块时发生错误: " + t.getMessage());
                        }

                        @Override
                        public void onCompleted() {
                            logger.info("数据块写入完成");
                        }
                    });

            // 发送元数据
            Dataserver.WriteBlockMetadata metadata = Dataserver.WriteBlockMetadata.newBuilder()
                    .setBlockId(blockId)
                    .addAllReplicaLocations(replicaLocations)
                    .build();

            Dataserver.WriteBlockRequest metadataRequest = Dataserver.WriteBlockRequest.newBuilder()
                    .setMetadata(metadata)
                    .build();
            requestObserver.onNext(metadataRequest);

            // 分块发送数据
            int chunkSize = 64 * 1024; // 64KB per chunk
            for (int i = 0; i < data.length; i += chunkSize) {
                int end = Math.min(i + chunkSize, data.length);
                byte[] chunk = Arrays.copyOfRange(data, i, end);

                Dataserver.WriteBlockRequest dataRequest = Dataserver.WriteBlockRequest.newBuilder()
                        .setChunkData(ByteString.copyFrom(chunk))
                        .build();
                requestObserver.onNext(dataRequest);
            }

            requestObserver.onCompleted();
            logger.info("向DataServer写入数据块成功: " + dataServerAddress + ", 块ID: " + blockId + ", 大小: " + data.length);
            return true;

        } catch (Exception e) {
            logger.severe("向DataServer写入数据块时发生异常: " + e.getMessage());
            return false;
        } finally {
            if (dataChannel != null && !dataChannel.isShutdown()) {
                try {
                    dataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * 删除DataServer上的数据块
     * @param dataServerAddress DataServer地址
     * @param blockId 块ID
     * @return 操作结果
     */
    public boolean deleteBlockFromDataServer(String dataServerAddress, long blockId) {
        ManagedChannel dataChannel = null;
        try {
            String[] parts = dataServerAddress.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            dataChannel = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .build();

            DataServerServiceGrpc.DataServerServiceBlockingStub dataStub =
                    DataServerServiceGrpc.newBlockingStub(dataChannel);

            Dataserver.DeleteBlockRequest request = Dataserver.DeleteBlockRequest.newBuilder()
                    .setBlockId(blockId)
                    .build();

            Dataserver.DeleteBlockResponse response = dataStub.deleteBlock(request);
            boolean success = response.getSuccess();

            if (success) {
                logger.info("从DataServer删除数据块成功: " + dataServerAddress + ", 块ID: " + blockId);
            } else {
                logger.warning("从DataServer删除数据块失败: " + dataServerAddress + ", 块ID: " + blockId);
            }

            return success;

        } catch (Exception e) {
            logger.severe("从DataServer删除数据块时发生异常: " + e.getMessage());
            return false;
        } finally {
            if (dataChannel != null && !dataChannel.isShutdown()) {
                try {
                    dataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * 复制DataServer上的数据块
     * @param targetAddress 目标DataServer地址
     * @param blockId 块ID
     * @param sourceAddress 源DataServer地址
     * @return 操作结果
     */
    public boolean copyBlockBetweenDataServers(String targetAddress, long blockId, String sourceAddress) {
        ManagedChannel dataChannel = null;
        try {
            String[] parts = targetAddress.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            dataChannel = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .build();

            DataServerServiceGrpc.DataServerServiceBlockingStub dataStub =
                    DataServerServiceGrpc.newBlockingStub(dataChannel);

            Dataserver.CopyBlockRequest request = Dataserver.CopyBlockRequest.newBuilder()
                    .setBlockId(blockId)
                    .setSourceAddress(sourceAddress)
                    .build();

            Dataserver.CopyBlockResponse response = dataStub.copyBlock(request);
            boolean success = response.getSuccess();

            if (success) {
                logger.info("复制数据块成功: 从 " + sourceAddress + " 到 " + targetAddress + ", 块ID: " + blockId);
            } else {
                logger.warning("复制数据块失败: 从 " + sourceAddress + " 到 " + targetAddress + ", 块ID: " + blockId);
            }

            return success;

        } catch (Exception e) {
            logger.severe("复制数据块时发生异常: " + e.getMessage());
            return false;
        } finally {
            if (dataChannel != null && !dataChannel.isShutdown()) {
                try {
                    dataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * 计算数据的MD5哈希值
     * @param data 数据内容
     * @return MD5哈希字符串
     */
    private String calculateMD5(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hash = md.digest(data);
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            logger.severe("计算MD5时发生异常: " + e.getMessage());
            return "";
        }
    }


    /**
     * 获取集群信息
     * @return 集群信息，如果失败返回null
     */
    public Metaserver.ClusterInfo getClusterInfo() {
        return executeWithFailover(() -> {
            Metaserver.GetClusterInfoRequest request =
                    Metaserver.GetClusterInfoRequest.newBuilder().build();
            Metaserver.GetClusterInfoResponse response =
                    blockingStub.getClusterInfo(request);

            logger.info("获取集群信息成功，DataServer数量: "
                    + response.getClusterInfo().getDataServerCount());
            return response.getClusterInfo();
        }, null, "获取集群信息");
    }

    /**
     * 获取文件的副本分布情况
     * @param path 文件路径，为空表示查询所有文件
     * @return 副本信息，如果失败返回null
     */
    public Metaserver.GetReplicationInfoResponse getReplicationInfo(String path) {
        return executeWithFailover(() -> {
            Metaserver.GetReplicationInfoRequest request =
                    Metaserver.GetReplicationInfoRequest.newBuilder()
                            .setPath(path == null ? "" : path)
                            .build();

            Metaserver.GetReplicationInfoResponse response =
                    blockingStub.getReplicationInfo(request);

            logger.info("获取副本信息成功，总文件数: " + response.getTotalFiles() +
                    ", 健康文件数: " + response.getHealthyFiles() +
                    ", 副本不足文件数: " + response.getUnderReplicatedFiles() +
                    ", 副本过多文件数: " + response.getOverReplicatedFiles());
            return response;
        }, null, "获取副本信息" + (path != null && !path.isEmpty() ? "[" + path + "]" : "[所有文件]"));
    }

    /**
     * 获取所有文件的副本分布情况
     * @return 副本信息，如果失败返回null
     */
    public Metaserver.GetReplicationInfoResponse getAllReplicationInfo() {
        return getReplicationInfo("");
    }

    /**
     * 获取元数据服务器地址
     * @return 元数据服务器地址
     */
    public String getMetaServerAddress() {
        if (useEtcd) {
            return serviceDiscovery.getAvailableMetaServer();
        }
        return metaServerAddress + ":" + port;
    }

    /**
     * 关闭客户端连接
     */
    @Override
    public void close() {
        try {
            if (channel != null && !channel.isShutdown()) {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            }

            // 关闭 etcd 服务发现
            if (serviceDiscovery != null) {
                serviceDiscovery.close();
            }

            logger.info("MinFS客户端已关闭");
        } catch (InterruptedException e) {
            logger.warning("关闭客户端时被中断: " + e.getMessage());
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.warning("关闭客户端时发生异常: " + e.getMessage());
        }
    }
}
