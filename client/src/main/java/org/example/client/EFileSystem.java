package org.example.client;

import dfs_project.Metaserver;
import org.example.client.client.MinFSClient;
import org.example.client.domain.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
public class EFileSystem extends FileSystem{

    private final MinFSClient client;

    /* 无参构造：默认 etcd 配置 */
    public EFileSystem() {
        this("default");
    }

    /* 带命名空间构造：真正初始化 client */
    public EFileSystem(String fileSystemName) {
        this.defaultFileSystemName = fileSystemName;
        this.client = new MinFSClient("http://localhost:2379", "/minfs", true);
    }

    public FSInputStream open(String path) throws IOException {
        Metaserver.StatInfo statInfo = client.getStatus(path);
        if (statInfo == null || statInfo.getType() != Metaserver.FileType.File) {
            throw new IOException("File not found or is not a regular file: " + path);
        }
        return new FSInputStream(client, path, statInfo.getSize());
    }

    public FSOutputStream create(String path) throws IOException {
        if (!client.createFile(path)) {
            throw new IOException("Failed to create file: " + path);
        }
        return new FSOutputStream(client, path);
    }

    /**
     * 创建目录
     */
    public boolean mkdir(String path){
        //创建目录
        return client.createDirectory(path);
    }

    /**
     * 删除文件
     */
    public boolean delete(String path){
        Metaserver.StatInfo status = client.getStatus(path);
        Metaserver.FileType type = status.getType();
        if(Metaserver.FileType.File.equals(type)){
            return client.deleteFile(path);
        } else if (Metaserver.FileType.Directory.equals(type)) {
            return client.deleteDirectory(path);
        }
        return false;
    }

    /**
     * 获取文件或目录的属性信息
     */
    public StatInfo getFileStats(String path){
        //获取文件状态
        Metaserver.StatInfo statInfo = client.getStatus(path);
        if (statInfo == null) {
            return null; // 文件不存在时返回null
        }
        StatInfo statInfo1=new StatInfo();
        statInfo1.setPath(statInfo.getPath());
        statInfo1.setSize(statInfo.getSize());
        statInfo1.setMtime(statInfo.getMtime());
        // 转换FileType枚举
        statInfo1.setType(convertProtoFileType(statInfo.getType()));
        // 获取副本数据（通过getReplicationInfo）
        List<ReplicaData> replicaDataList = new ArrayList<>();
        try {
            Metaserver.GetReplicationInfoResponse replInfo = client.getReplicationInfo(path);
            if (replInfo != null && replInfo.getFilesCount() > 0) {
                // 查找匹配路径的文件
                for (Metaserver.ReplicationStatus fileStatus : replInfo.getFilesList()) {
                    if (fileStatus.getPath().equals(path)) {
                        // 遍历该文件的所有数据块
                        for (Metaserver.BlockReplicationInfo blockInfo : fileStatus.getBlocksList()) {
                            String blockId = String.valueOf(blockInfo.getBlockId());
                            // 为每个副本位置创建ReplicaData
                            for (String location : blockInfo.getLocationsList()) {
                                ReplicaData replica = new ReplicaData();
                                replica.id = blockId;
                                replica.dsNode = location;
                                // 生成 /dataserver-X/块ID 格式的路径
                                String port = location.split(":")[1];
                                int serverNum = Integer.parseInt(port) - 8000; // 8001->1, 8002->2, 8003->3, 8004->4
                                replica.path = "/data" + serverNum + "/" + blockId;
                                replicaDataList.add(replica);
                            }
                        }
                        break;
                    }
                }
            }
        } catch (Exception e) {
            // 如果获取副本信息失败，继续返回基本信息
            System.err.println("获取副本信息失败: " + e.getMessage());
        }
        statInfo1.setReplicaData(replicaDataList);
        return statInfo1;
    }
    /**
     * 列出目录下的所有条目
     */
    public List<StatInfo> listFileStats(String path){
        List<Metaserver.StatInfo> statInfos = client.listStatus(path);
        if (statInfos == null) {
            return new ArrayList<>(); // 返回空列表而不是null
        }
        List<StatInfo> statInfos1=new ArrayList<>();
        // 逐个转换而不是直接copyProperties
        for (Metaserver.StatInfo statInfo : statInfos) {
            if (statInfo != null) {
                StatInfo newStatInfo = new StatInfo();
                newStatInfo.setPath(statInfo.getPath());
                newStatInfo.setSize(statInfo.getSize());
                newStatInfo.setMtime(statInfo.getMtime());
                // 转换FileType枚举
                newStatInfo.setType(convertProtoFileType(statInfo.getType()));
                // 复制副本数据
                List<ReplicaData> replicaDataList = new ArrayList<>();
                for (Metaserver.ReplicaData protoReplica : statInfo.getReplicaDataList()) {
                    ReplicaData replica = new ReplicaData();
                    replica.setId(protoReplica.getId());
                    replica.setDsNode(protoReplica.getDsNode());
                    replica.setPath(protoReplica.getPath());
                    replicaDataList.add(replica);
                }
                newStatInfo.setReplicaData(replicaDataList);
                statInfos1.add(newStatInfo);
            }
        }
        return statInfos1;
    }

    /**
     * 获取集群信息
     */
    public ClusterInfo getClusterInfo(){
        Metaserver.ClusterInfo clusterInfo = client.getClusterInfo();
        ClusterInfo clusterInfo1 = new ClusterInfo();
        
        // 手动转换数据，因为 protobuf 类和自定义类结构不同
        // TODO: 实现具体的转换逻辑
        
        return clusterInfo1;
    }

    /**
     * 获取内部的MinFSClient实例（用于高级操作）
     */
    public MinFSClient getClient() {
        return client;
    }

    /**
     * 转换proto FileType到domain FileType
     */
    private FileType convertProtoFileType(Metaserver.FileType protoType) {
        switch (protoType) {
            case Volume:
                return FileType.Volume;
            case File:
                return FileType.File;
            case Directory:
                return FileType.Directory;
            default:
                return FileType.Unknown;
        }
    }
}
