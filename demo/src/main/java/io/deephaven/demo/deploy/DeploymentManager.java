package io.deephaven.demo.deploy;

import io.deephaven.demo.ClusterController;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

/**
 * A DeploymentManager is responsible for creating and interacting with real VMs.
 *
 * We are using this abstraction layer to push ALL google-cloud-specific code into a single box,
 * so that it can be replaced with alternate implementations when (and if) a customer ever uses this code.
 */
interface DeploymentManager {

    void assignDns(final ClusterController ctrl, Stream<Machine> nodes) throws IOException, InterruptedException, TimeoutException;
    void createMachine(Machine node, final IpPool ips) throws IOException, InterruptedException;
    default void destroyCluster(Machine machine) throws IOException {
        destroyCluster(Collections.singleton(machine), "");
    }
    void destroyCluster(Collection<Machine> allNodes, String diskPrefix) throws IOException;
    boolean turnOn(Machine node) throws IOException, InterruptedException;
    boolean turnOff(Machine worker) throws IOException, InterruptedException;

    void createSnapshot(String snapshotName, ClusterMap map, boolean forceCreate, String prefix) throws IOException, InterruptedException;
    void restoreSnapshot(String snapshotName, ClusterMap map, boolean restart, String prefix);
    Collection<String> findMissingSnapshots(String snapshotName, ClusterMap map);
    void waitForSsh(Machine node);

    Collection<IpMapping> requestNewIps(int i);

    void waitUntilIpsCreated();
}

