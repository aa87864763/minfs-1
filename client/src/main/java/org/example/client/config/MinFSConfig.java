package org.example.client.config;

import org.example.client.client.MinFSClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * MinFS客户端配置类
 */
@Configuration
public class MinFSConfig {

    @Value("${minfs.meta-server.address:localhost}")
    private String metaServerAddress;

    @Value("${minfs.meta-server.port:9091}")
    private int metaServerPort;

    @Value("${minfs.etcd.enabled:false}")
    private boolean etcdEnabled;

    @Value("${minfs.etcd.endpoints:http://10.212.217.58:2379}")
    private String etcdEndpoints;

    @Value("${minfs.etcd.base-path:/minfs}")
    private String etcdBasePath;

    /**
     * 创建MinFS客户端Bean
     */
    @Bean
    public MinFSClient minFSClient() {
        if (etcdEnabled) {
            // 使用 etcd 服务发现
            return new MinFSClient(etcdEndpoints, etcdBasePath, true);
        } else {
            // 使用固定地址
            return new MinFSClient(metaServerAddress, metaServerPort);
        }
    }
}
