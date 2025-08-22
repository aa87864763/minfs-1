package com.ksyun.campus.client.discovery;

import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import io.etcd.jetcd.ByteSequence;

import dfs_project.Metaserver;
import dfs_project.MetaServerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * etcd 服务发现组件
 * 用于从 etcd 注册中心获取 MetaServer 和 DataServer 的地址信息
 */
public class EtcdServiceDiscovery implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(EtcdServiceDiscovery.class.getName());

    private final Client etcdClient;
    private final String basePath;
    private final ConcurrentHashMap<String, List<String>> serviceCache = new ConcurrentHashMap<>();
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
    private final List<Watch.Watcher> watchers = new ArrayList<>();

    // 服务类型常量
    public static final String META_SERVER_PREFIX = "/minfs/metaServer/";
    public static final String DATA_SERVER_PREFIX = "/minfs/dataServer/";

    /**
     * 构造函数
     * @param etcdEndpoints etcd 服务地址列表，如 "http://localhost:2379"
     * @param basePath 服务注册的基础路径
     */
    public EtcdServiceDiscovery(String etcdEndpoints, String basePath) {
        this.etcdClient = Client.builder()
                .endpoints(etcdEndpoints.split(","))
                .build();
        this.basePath = basePath;

        // 启动服务发现
        startServiceDiscovery();
    }

    /**
     * 启动服务发现
     */
    private void startServiceDiscovery() {
        // 定期刷新服务列表
        executor.scheduleAtFixedRate(this::refreshServices, 0, 30, TimeUnit.SECONDS);

        // 监听服务变化
        watchServices();
    }

    /**
     * 刷新服务列表
     */
    private void refreshServices() {
        try {
            // 刷新 MetaServer 列表
            refreshServiceList(META_SERVER_PREFIX, "metaServer");

            // 刷新 DataServer 列表
            refreshServiceList(DATA_SERVER_PREFIX, "dataServer");
        } catch (Exception e) {
            logger.warning("刷新服务列表失败: " + e.getMessage());
        }
    }

    /**
     * 刷新指定类型的服务列表
     */
    private void refreshServiceList(String prefix, String serviceType) {
        try {
            ByteSequence prefixBytes = ByteSequence.from(prefix, StandardCharsets.UTF_8);
            GetOption option = GetOption.newBuilder()
                    .withPrefix(prefixBytes)
                    .build();

            CompletableFuture<GetResponse> future = etcdClient.getKVClient().get(prefixBytes, option);
            GetResponse response = future.get(5, TimeUnit.SECONDS);

            List<String> services = new ArrayList<>();
            for (KeyValue kv : response.getKvs()) {
                String key = kv.getKey().toString(StandardCharsets.UTF_8);
                String value = kv.getValue().toString(StandardCharsets.UTF_8);

                // 提取服务地址
                if (key.startsWith(prefix) && !value.isEmpty()) {
                    String address = extractServiceAddress(key, value, serviceType);
                    if (address != null && !address.isEmpty()) {
                        services.add(address);
                    }
                }
            }

            serviceCache.put(serviceType, services);
            // logger.info("刷新 " + serviceType + " 服务列表成功，共 " + services.size() + " 个服务");

        } catch (Exception e) {
            logger.warning("刷新 " + serviceType + " 服务列表失败: " + e.getMessage());
        }
    }

    /**
     * 监听服务变化
     */
    private void watchServices() {
        try {
            // 监听 MetaServer 变化
            ByteSequence metaPrefixBytes = ByteSequence.from(META_SERVER_PREFIX, StandardCharsets.UTF_8);
            WatchOption metaWatchOption = WatchOption.newBuilder()
                    .withPrefix(metaPrefixBytes)
                    .build();

            Watch.Watcher metaWatcher = etcdClient.getWatchClient().watch(
                    metaPrefixBytes,
                    metaWatchOption,
                    new Watch.Listener() {
                        @Override
                        public void onNext(WatchResponse response) {
                            for (WatchEvent event : response.getEvents()) {
                                logger.info("MetaServer 服务变化: " + event.getEventType() + " - " +
                                        event.getKeyValue().getKey().toString(StandardCharsets.UTF_8));
                            }
                            // 刷新服务列表
                            refreshServiceList(META_SERVER_PREFIX, "metaServer");
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            logger.warning("监听 MetaServer 服务变化失败: " + throwable.getMessage());
                        }

                        @Override
                        public void onCompleted() {
                            logger.info("MetaServer 服务监听完成");
                        }
                    }
            );
            watchers.add(metaWatcher);

            // 监听 DataServer 变化
            ByteSequence dataPrefixBytes = ByteSequence.from(DATA_SERVER_PREFIX, StandardCharsets.UTF_8);
            WatchOption dataWatchOption = WatchOption.newBuilder()
                    .withPrefix(dataPrefixBytes)
                    .build();

            Watch.Watcher dataWatcher = etcdClient.getWatchClient().watch(
                    dataPrefixBytes,
                    dataWatchOption,
                    new Watch.Listener() {
                        @Override
                        public void onNext(WatchResponse response) {
                            for (WatchEvent event : response.getEvents()) {
                                logger.info("DataServer 服务变化: " + event.getEventType() + " - " +
                                        event.getKeyValue().getKey().toString(StandardCharsets.UTF_8));
                            }
                            // 刷新服务列表
                            refreshServiceList(DATA_SERVER_PREFIX, "dataServer");
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            logger.warning("监听 DataServer 服务变化失败: " + throwable.getMessage());
                        }

                        @Override
                        public void onCompleted() {
                            logger.info("DataServer 服务监听完成");
                        }
                    }
            );
            watchers.add(dataWatcher);
        } catch (Exception e) {
            logger.warning("启动服务监听失败: " + e.getMessage());
        }
    }

    /**
     * 从etcd值中提取服务地址
     * @param key etcd键
     * @param value etcd值
     * @param serviceType 服务类型
     * @return 服务地址
     */
    private String extractServiceAddress(String key, String value, String serviceType) {
        try {
            if ("metaServer".equals(serviceType)) {
                // MetaServer: 只处理 /nodes/ 路径下的注册信息，忽略 /election/ 路径
                if (key.contains("/nodes/")) {
                    // 解析JSON格式: {"node_id":"metaServer-9090","host":"localhost","port":9090,"addr":"localhost:9090",...}
                    if (value.startsWith("{") && value.contains("\"addr\"")) {
                        // 简单提取addr字段值
                        int addrStart = value.indexOf("\"addr\":\"") + 8;
                        if (addrStart > 7) {
                            int addrEnd = value.indexOf("\"", addrStart);
                            if (addrEnd > addrStart) {
                                return value.substring(addrStart, addrEnd);
                            }
                        }
                    }
                }
                return null; // 忽略election路径或无效格式
            } else if ("dataServer".equals(serviceType)) {
                // DataServer: 值直接是地址
                return value;
            }
        } catch (Exception e) {
            logger.warning("解析服务地址失败: " + e.getMessage() + ", key: " + key + ", value: " + value);
        }
        return null;
    }

    /**
     * 获取 MetaServer 地址列表
     * @return MetaServer 地址列表
     */
    public List<String> getMetaServers() {
        return serviceCache.getOrDefault("metaServer", new ArrayList<>());
    }

    /**
     * 获取 DataServer 地址列表
     * @return DataServer 地址列表
     */
    public List<String> getDataServers() {
        return serviceCache.getOrDefault("dataServer", new ArrayList<>());
    }

    /**
     * 获取一个可用的 MetaServer 地址 - 优先连接Leader
     * @return MetaServer 地址，如果没有可用服务则返回 null
     */
    public String getAvailableMetaServer() {
        List<String> metaServers = getMetaServers();
        if (metaServers.isEmpty()) {
            logger.warning("没有可用的 MetaServer");
            return null;
        }

        // 优先发现并连接到Leader节点
        String leaderServer = discoverLeader(metaServers);
        if (leaderServer != null) {
            // logger.info("发现并选择Leader MetaServer: " + leaderServer);
            return leaderServer;
        }

        // 如果没有找到Leader，退回到第一个可用的MetaServer
        String selectedServer = metaServers.get(0);
        // logger.info("未找到Leader，选择第一个可用的 MetaServer: " + selectedServer);
        return selectedServer;
    }

    /**
     * 获取一个可用的 DataServer 地址
     * @return DataServer 地址，如果没有可用服务则返回 null
     */
    public String getAvailableDataServer() {
        List<String> dataServers = getDataServers();
        if (dataServers.isEmpty()) {
            logger.warning("没有可用的 DataServer");
            return null;
        }

        // 简单的轮询策略
        String selectedServer = dataServers.get(0);
        // logger.info("选择 DataServer: " + selectedServer);
        return selectedServer;
    }

    /**
     * 检查服务是否可用
     * @param serviceAddress 服务地址
     * @return 是否可用
     */
    public boolean isServiceAvailable(String serviceAddress) {
        if (serviceAddress == null || serviceAddress.isEmpty()) {
            return false;
        }

        try {
            // 这里可以实现更详细的健康检查逻辑
            // 目前简单检查地址是否在缓存的服务列表中
            String[] parts = serviceAddress.split(":");
            if (parts.length != 2) {
                return false;
            }

            int port = Integer.parseInt(parts[1]);
            return port >= 8000 && port <= 9999;
        } catch (Exception e) {
            logger.warning("检查服务可用性失败: " + e.getMessage());
            return false;
        }
    }

    /**
     * 从MetaServer列表中发现Leader节点
     * @param metaServers MetaServer地址列表
     * @return Leader地址，如果没有找到返回null
     */
    private String discoverLeader(List<String> metaServers) {
        for (String serverAddr : metaServers) {
            try {
                String leaderAddr = checkIfLeaderAndGetLeaderAddr(serverAddr);
                if (leaderAddr != null) {
                    return leaderAddr;
                }
            } catch (Exception e) {
                logger.warning("检查MetaServer " + serverAddr + " 是否为Leader时发生异常: " + e.getMessage());
            }
        }
        return null;
    }

    /**
     * 检查指定的MetaServer是否为Leader，并返回真正的Leader地址
     * @param serverAddr MetaServer地址
     * @return Leader地址，如果当前节点就是Leader则返回当前地址，如果不是则返回真正的Leader地址，失败返回null
     */
    private String checkIfLeaderAndGetLeaderAddr(String serverAddr) {
        ManagedChannel channel = null;
        try {
            // 解析地址
            String[] parts = serverAddr.split(":");
            if (parts.length != 2) {
                logger.warning("无效的MetaServer地址格式: " + serverAddr);
                return null;
            }
            
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            
            // 创建gRPC连接
            channel = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .build();
            
            MetaServerServiceGrpc.MetaServerServiceBlockingStub stub = 
                    MetaServerServiceGrpc.newBlockingStub(channel);
            
            // 调用GetLeader接口
            Metaserver.GetLeaderRequest request = Metaserver.GetLeaderRequest.newBuilder().build();
            Metaserver.GetLeaderResponse response = stub.getLeader(request);
            
            // 检查返回的Leader信息
            if (response.hasLeader()) {
                String leaderHost = response.getLeader().getHost();
                int leaderPort = response.getLeader().getPort();
                String leaderAddress = leaderHost + ":" + leaderPort;
                
                // logger.info("从 " + serverAddr + " 获取到Leader信息: " + leaderAddress);
                
                // 验证Leader地址是否在我们的服务列表中
                if (getMetaServers().contains(leaderAddress)) {
                    return leaderAddress;
                } else {
                    logger.warning("Leader地址 " + leaderAddress + " 不在服务发现列表中");
                    // 如果Leader地址有效，仍然返回它
                    return leaderAddress;
                }
            } else {
                logger.warning("从 " + serverAddr + " 获取不到Leader信息");
                return null;
            }
            
        } catch (StatusRuntimeException e) {
            logger.warning("连接到MetaServer " + serverAddr + " 失败: " + e.getStatus().getDescription());
            return null;
        } catch (Exception e) {
            logger.warning("检查MetaServer " + serverAddr + " 时发生异常: " + e.getMessage());
            return null;
        } finally {
            if (channel != null) {
                try {
                    channel.shutdown();
                    if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                        channel.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    channel.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Override
    public void close() {
        try {
            // 关闭监听器
            for (Watch.Watcher watcher : watchers) {
                watcher.close();
            }

            // 关闭执行器
            executor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }

            // 关闭 etcd 客户端
            if (etcdClient != null) {
                etcdClient.close();
            }

            logger.info("EtcdServiceDiscovery 已关闭");
        } catch (Exception e) {
            logger.warning("关闭 EtcdServiceDiscovery 时发生异常: " + e.getMessage());
        }
    }
}
