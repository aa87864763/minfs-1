package com.ksyun.campus.client.domain;

import java.util.List;

/**
 * @author zxa
 */
public class ClusterInfo {
    private MetaServerMsg masterMetaServer;
    private List<MetaServerMsg> slaveMetaServer;
    private List<DataServerMsg> dataServer;

    public List<MetaServerMsg> getSlaveMetaServer() {
        return slaveMetaServer;
    }

    public void setSlaveMetaServer(List<MetaServerMsg> slaveMetaServer) {
        this.slaveMetaServer = slaveMetaServer;
    }

    public MetaServerMsg getMasterMetaServer() {
        return masterMetaServer;
    }

    public void setMasterMetaServer(MetaServerMsg masterMetaServer) {
        this.masterMetaServer = masterMetaServer;
    }


    public List<DataServerMsg> getDataServer() {
        return dataServer;
    }

    public void setDataServer(List<DataServerMsg> dataServer) {
        this.dataServer = dataServer;
    }

    @Override
    public String toString() {
        return "ClusterInfo{" +
                "masterMetaServer=" + masterMetaServer +
                ", slaveMetaServer=" + slaveMetaServer +
                ", dataServer=" + dataServer +
                '}';
    }
}
