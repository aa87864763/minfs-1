package org.example.client;

import dfs_project.Metaserver;
import org.example.client.client.MinFSClient;
import org.example.client.domain.ClusterInfo;
import org.example.client.domain.StatInfo;
import org.springframework.beans.BeanUtils;

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
        BeanUtils.copyProperties(statInfo,statInfo1);
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
                BeanUtils.copyProperties(statInfo, newStatInfo);
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
        ClusterInfo clusterInfo1=new ClusterInfo();
        BeanUtils.copyProperties(clusterInfo,clusterInfo1);
        return clusterInfo1;
    }

    /**
     * 获取内部的MinFSClient实例（用于高级操作）
     */
    public MinFSClient getClient() {
        return client;
    }
}
