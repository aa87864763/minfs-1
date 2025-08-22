package com.ksyun.campus.client;

import dfs_project.Metaserver;
import com.ksyun.campus.client.client.MinFSClient;
import com.ksyun.campus.client.domain.*;

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

    public FSInputStream open(String path) {
        try {
            Metaserver.StatInfo statInfo = client.getStatus(path);
            if (statInfo == null) {
                System.err.println("[SDK] 打开失败：文件 " + path + " 不存在。");
                return null;
            }
            if (statInfo.getType() != Metaserver.FileType.File) {
                System.err.println("[SDK] 打开失败：" + path + " 不是文件。");
                return null;
            }
            System.out.println("[SDK] 文件打开成功: " + path + " (大小: " + statInfo.getSize() + " 字节)");
            return new FSInputStream(client, path, statInfo.getSize());
        } catch (Exception e) {
            System.err.println("[SDK] 打开失败：" + path + " - " + e.getMessage());
            return null;
        }
    }

    public FSOutputStream create(String path) {
        try {
            // 检查文件是否已存在
            Metaserver.StatInfo existingFile = client.getStatus(path);
            if (existingFile != null && existingFile.getType() == Metaserver.FileType.File) {
                // 文件已存在，先删除后重新创建（覆盖写）
                System.out.println("[SDK] 文件已存在，执行覆盖写: " + path);
                if (!client.deleteFile(path)) {
                    System.err.println("[SDK] 创建失败：无法删除已存在文件 " + path);
                    return null;
                }
            }
            
            // 创建文件
            if (!client.createFile(path)) {
                System.err.println("[SDK] 创建失败：无法创建文件 " + path);
                return null;
            }
            
            System.out.println("[SDK] 文件创建成功: " + path);
            return new FSOutputStream(client, path);
        } catch (Exception e) {
            System.err.println("[SDK] 创建失败：" + path + " - " + e.getMessage());
            return null;
        }
    }

    /**
     * 创建目录
     */
    public boolean mkdir(String path){
        try {
            // 检查目录是否已存在
            Metaserver.StatInfo existingDir = client.getStatus(path);
            if (existingDir != null && existingDir.getType() == Metaserver.FileType.Directory) {
                System.err.println("[SDK] 创建失败：目录 " + path + " 已存在。");
                return false;
            }
            
            boolean success = client.createDirectory(path);
            if (success) {
                System.out.println("[SDK] 目录创建成功: " + path);
            } else {
                System.err.println("[SDK] 创建失败：无法创建目录 " + path);
            }
            return success;
        } catch (Exception e) {
            System.err.println("[SDK] 创建失败：" + path + " - " + e.getMessage());
            return false;
        }
    }

    /**
     * 删除文件
     */
    public boolean delete(String path){
        try {
            Metaserver.StatInfo status = client.getStatus(path);
            if (status == null) {
                System.err.println("[SDK] 删除失败：文件或目录 " + path + " 不存在。");
                return false;
            }
            
            Metaserver.FileType type = status.getType();
            boolean success = false;
            
            if(Metaserver.FileType.File.equals(type)){
                success = client.deleteFile(path);
                if (success) {
                    System.out.println("[SDK] 文件删除成功: " + path);
                } else {
                    System.err.println("[SDK] 删除失败：无法删除文件 " + path);
                }
            } else if (Metaserver.FileType.Directory.equals(type)) {
                success = client.deleteDirectory(path);
                if (success) {
                    System.out.println("[SDK] 目录删除成功: " + path);
                } else {
                    System.err.println("[SDK] 删除失败：无法删除目录 " + path);
                }
            } else {
                System.err.println("[SDK] 删除失败：不支持的文件类型 " + type);
            }
            return success;
        } catch (Exception e) {
            System.err.println("[SDK] 删除失败：" + path + " - " + e.getMessage());
            return false;
        }
    }

    /**
     * 获取文件或目录的属性信息
     */
    public StatInfo getFileStats(String path){
        try {
            Metaserver.StatInfo statInfo = client.getStatus(path);
            if (statInfo == null) {
                System.err.println("[SDK] 获取状态失败：文件或目录 " + path + " 不存在。");
                return null;
            }
            System.out.println("[SDK] 获取文件状态成功: " + path);
            System.out.println("路径: " + statInfo.getPath());
            System.out.println("类型: " + convertProtoFileType(statInfo.getType()));
            System.out.println("大小: " + statInfo.getSize() + " 字节");
            System.out.println("修改时间: " + statInfo.getMtime());
        
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
                                // 生成 /dataServer-X/块ID 格式的路径
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
            System.err.println("[SDK] 获取副本信息失败: " + e.getMessage());
        }
        statInfo1.setReplicaData(replicaDataList);
        
        // 在SDK内部打印副本信息
        if (!replicaDataList.isEmpty()) {
            System.out.println("副本信息:");
            for (ReplicaData replica : replicaDataList) {
                System.out.println("  " + replica.toString());
            }
        }
        
        return statInfo1;
        } catch (Exception e) {
            System.err.println("[SDK] 获取状态失败：" + path + " - " + e.getMessage());
            return null;
        }
    }
    /**
     * 列出目录下的所有条目
     */
    public List<StatInfo> listFileStats(String path){
        try {
            List<Metaserver.StatInfo> statInfos = client.listStatus(path);
            if (statInfos == null) {
                System.err.println("[SDK] 列出目录失败：目录 " + path + " 不存在或为空。");
                return new ArrayList<>();
            }
            System.out.println("[SDK] 列出目录成功: " + path + " (" + statInfos.size() + " 个项目)");
            
            // 在SDK内部打印目录内容
            for (Metaserver.StatInfo statInfo : statInfos) {
                if (statInfo != null) {
                    String type = (statInfo.getType() == Metaserver.FileType.Directory) ? "DIR " : "FILE";
                    System.out.println("  " + type + " " + statInfo.getPath() + 
                        " (大小: " + statInfo.getSize() + ", 修改时间: " + statInfo.getMtime() + ")");
                }
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
        } catch (Exception e) {
            System.err.println("[SDK] 列出目录失败：" + path + " - " + e.getMessage());
            return new ArrayList<>();
        }
    }

    /**
     * 获取集群信息
     */
    public ClusterInfo getClusterInfo(){
        try {
            Metaserver.ClusterInfo protoClusterInfo = client.getClusterInfo();
            if (protoClusterInfo == null) {
                System.err.println("[SDK] 获取集群信息失败：无法连接到集群。");
                return null;
            }
            
            ClusterInfo clusterInfo = new ClusterInfo();
            
            // 转换主MetaServer信息
            if (protoClusterInfo.hasMasterMetaServer()) {
                MetaServerMsg masterMeta = new MetaServerMsg();
                masterMeta.setHost(protoClusterInfo.getMasterMetaServer().getHost());
                masterMeta.setPort(protoClusterInfo.getMasterMetaServer().getPort());
                clusterInfo.setMasterMetaServer(masterMeta);
            }
            
            // 转换从MetaServer信息
            List<MetaServerMsg> slaveMetaServers = new ArrayList<>();
            for (int i = 0; i < protoClusterInfo.getSlaveMetaServerCount(); i++) {
                MetaServerMsg slaveMeta = new MetaServerMsg();
                slaveMeta.setHost(protoClusterInfo.getSlaveMetaServer(i).getHost());
                slaveMeta.setPort(protoClusterInfo.getSlaveMetaServer(i).getPort());
                slaveMetaServers.add(slaveMeta);
            }
            clusterInfo.setSlaveMetaServer(slaveMetaServers);
            
            // 转换DataServer信息
            List<DataServerMsg> dataServers = new ArrayList<>();
            for (int i = 0; i < protoClusterInfo.getDataServerCount(); i++) {
                var protoDs = protoClusterInfo.getDataServer(i);
                DataServerMsg dataServer = new DataServerMsg();
                dataServer.setHost(protoDs.getHost());
                dataServer.setPort(protoDs.getPort());
                dataServer.setCapacity(protoDs.getCapacity());
                dataServer.setUseCapacity(protoDs.getUseCapacity());
                dataServer.setFileTotal(protoDs.getFileTotal());
                dataServers.add(dataServer);
            }
            clusterInfo.setDataServer(dataServers);
            
            System.out.println("[SDK] 获取集群信息成功。");
            
            // 在SDK内部打印集群信息
            System.out.println("=== 集群信息 ===");
            
            // 主MetaServer信息
            if (protoClusterInfo.hasMasterMetaServer()) {
                System.out.println("主MetaServer: " + 
                    protoClusterInfo.getMasterMetaServer().getHost() + ":" + 
                    protoClusterInfo.getMasterMetaServer().getPort());
            }
            
            // 从MetaServer信息
            if (protoClusterInfo.getSlaveMetaServerCount() > 0) {
                System.out.println("从MetaServer数量: " + protoClusterInfo.getSlaveMetaServerCount());
                for (int i = 0; i < protoClusterInfo.getSlaveMetaServerCount(); i++) {
                    System.out.println("  " + (i+1) + ". " + 
                        protoClusterInfo.getSlaveMetaServer(i).getHost() + ":" + 
                        protoClusterInfo.getSlaveMetaServer(i).getPort());
                }
            }
            
            // DataServer信息
            if (protoClusterInfo.getDataServerCount() > 0) {
                System.out.println("DataServer数量: " + protoClusterInfo.getDataServerCount());
                System.out.println("DataServer详情:");
                for (int i = 0; i < protoClusterInfo.getDataServerCount(); i++) {
                    var ds = protoClusterInfo.getDataServer(i);
                    System.out.println("  " + (i+1) + ". " + ds.getHost() + ":" + ds.getPort() +
                        " (文件: " + ds.getFileTotal() + ", 容量: " + ds.getUseCapacity() + "/" + ds.getCapacity() + "MB)");
                }
            }
            return clusterInfo;
        } catch (Exception e) {
            System.err.println("[SDK] 获取集群信息失败：" + e.getMessage());
            return null;
        }
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
