package io.deephaven.demo.api;

import java.util.List;

/**
 * ClusterMap:
 * <p>
 * <p> A container for 1 or more Machines that we want to operate on.
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
