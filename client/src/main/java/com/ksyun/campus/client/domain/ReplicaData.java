package com.ksyun.campus.client.domain;

public class ReplicaData {
    public String id;
    public String dsNode;//格式为ip:port
    public String path;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDsNode() {
        return dsNode;
    }

    public void setDsNode(String dsNode) {
        this.dsNode = dsNode;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @Override
    public String toString() {
        return "ReplicaData{" +
                "id='" + id + '\'' +
                ", dsNode='" + dsNode + '\'' +
                ", path='" + path + '\'' +
                '}';
    }
}
