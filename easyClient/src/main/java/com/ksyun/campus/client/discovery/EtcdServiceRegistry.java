package com.ksyun.campus.client.discovery;

import io.etcd.jetcd.Client;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.lease.LeaseRevokeResponse;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.ByteSequence;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * etcd 服务注册组件
 * 用于将服务信息注册到 etcd
 */
public class EtcdServiceRegistry implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(EtcdServiceRegistry.class.getName());
    
    private final Client etcdClient;
    private final String basePath;
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    private long leaseId = -1;
    private final String serviceId;
    private final String serviceAddress;
    private final String serviceType;
    
    /**
     * 构造函数
     * @param etcdEndpoints etcd 服务地址列表，如 "http://localhost:2379"
     * @param basePath 服务注册的基础路径
     * @param serviceId 服务ID
     * @param serviceAddress 服务地址（IP:Port）
     * @param serviceType 服务类型（metaServer 或 dataServer）
     */
    public EtcdServiceRegistry(String etcdEndpoints, String basePath, String serviceId, 
                              String serviceAddress, String serviceType) {
        this.etcdClient = Client.builder()
                .endpoints(etcdEndpoints.split(","))
                .build();
        this.basePath = basePath;
        this.serviceId = serviceId;
        this.serviceAddress = serviceAddress;
        this.serviceType = serviceType;
    }
    
    /**
     * 注册服务
     */
    public void register() {
        try {
            // 创建租约
            CompletableFuture<LeaseGrantResponse> leaseFuture = etcdClient.getLeaseClient()
                    .grant(30); // 30秒租约
            LeaseGrantResponse leaseResponse = leaseFuture.get(5, TimeUnit.SECONDS);
            this.leaseId = leaseResponse.getID();
            
            // 构建服务注册路径
            String serviceKey = basePath + "/" + serviceType + "s/" + serviceId;
            ByteSequence keyBytes = ByteSequence.from(serviceKey, StandardCharsets.UTF_8);
            ByteSequence valueBytes = ByteSequence.from(serviceAddress, StandardCharsets.UTF_8);
            
            // 注册服务
            PutOption putOption = PutOption.newBuilder()
                    .withLeaseId(leaseId)
                    .build();
            
            CompletableFuture<PutResponse> putFuture = etcdClient.getKVClient()
                    .put(keyBytes, valueBytes, putOption);
            putFuture.get(5, TimeUnit.SECONDS);
            
            logger.info("服务注册成功: " + serviceKey + " -> " + serviceAddress);
            
            // 启动心跳续约
            startHeartbeat();
            
        } catch (Exception e) {
            logger.severe("服务注册失败: " + e.getMessage());
            throw new RuntimeException("服务注册失败", e);
        }
    }
    
    /**
     * 启动心跳续约
     */
    private void startHeartbeat() {
        executor.scheduleAtFixedRate(() -> {
            try {
                if (leaseId > 0) {
                    // 续约
                    CompletableFuture<LeaseKeepAliveResponse> keepAliveFuture = etcdClient.getLeaseClient()
                            .keepAliveOnce(leaseId);
                    keepAliveFuture.get(5, TimeUnit.SECONDS);
                    logger.fine("服务心跳续约成功: " + serviceId);
                }
            } catch (Exception e) {
                logger.warning("服务心跳续约失败: " + e.getMessage());
                // 尝试重新注册
                try {
                    register();
                } catch (Exception re) {
                    logger.severe("重新注册服务失败: " + re.getMessage());
                }
            }
        }, 10, 10, TimeUnit.SECONDS); // 每10秒续约一次
    }
    
    /**
     * 注销服务
     */
    public void unregister() {
        try {
            if (leaseId > 0) {
                // 撤销租约
                CompletableFuture<LeaseRevokeResponse> revokeFuture = etcdClient.getLeaseClient()
                        .revoke(leaseId);
                revokeFuture.get(5, TimeUnit.SECONDS);
                logger.info("服务注销成功: " + serviceId);
            }
        } catch (Exception e) {
            logger.warning("服务注销失败: " + e.getMessage());
        }
    }
    
    @Override
    public void close() {
        try {
            // 注销服务
            unregister();
            
            // 关闭执行器
            executor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
            
            // 关闭 etcd 客户端
            if (etcdClient != null) {
                etcdClient.close();
            }
            
            logger.info("EtcdServiceRegistry 已关闭");
        } catch (Exception e) {
            logger.warning("关闭 EtcdServiceRegistry 时发生异常: " + e.getMessage());
        }
    }
}
