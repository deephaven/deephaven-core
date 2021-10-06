package io.deephaven.demo.deploy;

// this package name is intentional. it works better w/ quarkus native compiler
import io.deephaven.demo.ClusterController;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * IpPool:
 * <p>
 * <p> A collection of pre-reserved static IPs, so we can prepare multiple DNS records at once for a single IP.
 * <p> This will let us avoid the longest possible wait time during new worker creation: DNS-propagation delays.
 * <p>
 * <p> By having M (~30) unused IP addresses with N (3-5) live DNS records per IP,
 * <p> we can throw away DNS after each use, and still service at least M more requests with ~0 latency.
 * <p> Whenever we throw away N DNS records, just create N more fresh, unused ones.
 * <p>
 * <p> This class contains our read-from-google copy of those mappings, with our own internal state tracking on them.
 * <p>
 * <p>
 * <p> Each IP can (currently) only be run by a single instance;
 * <p> we could back them by something with a load balancer some day,
 * <p> but, for now, when a new client comes in, we first reserve an unassigned IP from the controller.
 * <p>
 * <p> Note: If there is a live machine already running with the given IP, we should (but don't, yet) tell that machine
 * its expected hostname and have it only honor requests to that domain. We could then report suspicious requests
 * that may be trying to DNS-scan pre-allocated routes to live hosts.
 * <p>
 */
public class IpPool {
    private final ConcurrentMap<String, IpMapping> allIps;
    private final SortedSet<IpMapping> used;
    private final SortedSet<IpMapping> unused;

    public IpPool() {
        allIps = new ConcurrentHashMap<>();
        used = new ConcurrentSkipListSet<>(IpMapping::compareTo);
        unused = new ConcurrentSkipListSet<>(IpMapping::compareTo);
    }

    public void addIpUnused(IpMapping ip) {
        allIps.put(ip.getName(), ip);
        allIps.put(ip.getIp(), ip);
        unused.add(ip);
    }

    public void addIpUsed(IpMapping ip) {
        allIps.put(ip.getName(), ip);
        allIps.put(ip.getIp(), ip);
        used.add(ip);
    }

    public IpMapping getUnusedIp(ClusterController ctrl) {
        final IpMapping ip;
        synchronized (unused) {
            if (unused.isEmpty()) {
                // uh-oh! no more IPs... ask for at least one new IP, scaling up by square root of total IPs allocated
                ctrl.requestNewIps((int) (1 + Math.sqrt(allIps.size())))
                        .forEach(this::addIpUnused);
            }
            final Iterator<IpMapping> itr = unused.iterator();
            ip = itr.next();
            itr.remove();
            used.add(ip);
        }
        return ip;
    }

    public IpMapping reserveIp(ClusterController ctrl, Machine node, IpMapping backup) {
        final String nodeIp = node.getIp();
        IpMapping ip = allIps.get(nodeIp);
        if (ip == null) {
            ip = backup;
        }
        if (ip == null) {
            ip = getUnusedIp(ctrl);
        }
        boolean alreadyRunning = ip.isRunningFor(node);
        changeState(ip, alreadyRunning ? IpState.Running : IpState.Claimed);
        ip.setInstance(node);
        used.add(ip);
        unused.remove(ip);
        return ip;
    }

    private void changeState(final IpMapping ip, final IpState state) {
        // TODO: validate all state changes
        ip.setState(state);
    }

    public int getNumUnused() {
        return unused.size();
    }

    public int getNumUsed() {
        return used.size();
    }
}
