package io.deephaven.demo.deploy;

import java.util.List;

/**
 * ClusterMap:
 * <p>
 * <p>
 * Created by James X. Nelson (James@WeTheInter.net) on 20/09/2021 @ 5:14 p.m..
 */
public class ClusterMap {
    private List<Machine> allNodes;
    private String localDir;
    private String clusterName;

    public List<Machine> getAllNodes() {
        return allNodes;
    }

    public void setAllNodes(final List<Machine> allNodes) {
        this.allNodes = allNodes;
    }

    public String getLocalDir() {
        return localDir;
    }

    public void setLocalDir(final String localDir) {
        this.localDir = localDir;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(final String clusterName) {
        this.clusterName = clusterName;
    }
}
