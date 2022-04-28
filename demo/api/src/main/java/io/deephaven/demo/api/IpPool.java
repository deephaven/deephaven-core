package io.deephaven.demo.api;

import io.deephaven.demo.manager.DeploymentManager;
import io.deephaven.demo.manager.Execute;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.SortedSet;
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

    private static final Logger LOG = Logger.getLogger(IpPool.class);

    private final ConcurrentMap<String, IpMapping> allIps;
    private final SortedSet<IpMapping> used;
    private final SortedSet<IpMapping> unused;

    public IpPool() {
        allIps = new ConcurrentHashMap<>();
        used = new ConcurrentSkipListSet<>(IpMapping::compareTo);
        unused = new ConcurrentSkipListSet<>(IpMapping::compareTo);
    }

    public void addIpUnused(IpMapping ip) {
        ip.setState(IpState.Unclaimed);
        allIps.put(ip.getName(), ip);
        if (ip.getIp() != null) {
            allIps.put(ip.getIp(), ip);
        }
        used.remove(ip);
        unused.add(ip);
    }

    public void addIpUsed(IpMapping ip) {
        ip.setState(IpState.Claimed);
        allIps.put(ip.getName(), ip);
        allIps.put(ip.getIp(), ip);
        unused.remove(ip);
        used.add(ip);
    }

    public IpMapping getUnusedIp(DeploymentManager manager) {
        IpMapping ip = null;
        synchronized (unused) {
            if (unused.isEmpty()) {
                // uh-oh! no more IPs... ask for at least one new IP, scaling up by square root of total IPs allocated
                manager.requestNewIps((int) (1 + Math.sqrt(allIps.size())))
                        .forEach(this::addIpUnused);
            }
            for (final Iterator<IpMapping> itr = unused.iterator(); itr.hasNext();) {
                ip = itr.next();
                if (ip.getState() == IpState.Unverified) {
                    // schedule unverified IP to be checked for existence
                    manager.checkIpState(this, ip);
                } else {
                    itr.remove();
                    used.add(ip);
                    break;
                }
            }
        }

        return ip;
    }

    public IpMapping reserveIp(DeploymentManager ctrl, Machine node) {
        IpMapping nodeIp = node.getIp();
        if (nodeIp == null ||
                (nodeIp.getState() != IpState.Unclaimed && (nodeIp.getInstance().orElse(null) != node))
        ) {
            LOG.infof("Machine %s had unusable IP %s, getting a new one", node.getHost(), nodeIp);
            nodeIp = getUnusedIp(ctrl);
            node.setIp(nodeIp);
        }
        boolean alreadyRunning = nodeIp.isRunningFor(node);
        changeState(nodeIp, alreadyRunning ? IpState.Running : IpState.Claimed);

        if (nodeIp.getIp() != null) {
            allIps.put(nodeIp.getIp(), nodeIp);
        }
        allIps.put(nodeIp.getName(), nodeIp);
        // must remove ip from both sets before we call setInstance, which updates timestamp
        used.remove(nodeIp);
        unused.remove(nodeIp);
        nodeIp.setInstance(node);
        used.add(nodeIp);
        return nodeIp;
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

    public IpMapping updateOrCreate(final String name, final String addr) {
        final IpMapping ip = allIps.computeIfAbsent(name, n -> new IpMapping(name, addr));
        ip.setIp(addr);
        if (addr != null) {
            allIps.put(addr, ip);
        }
        return ip;
    }

    public boolean hasIp(final String ipAddr) {
        return allIps.containsKey(ipAddr);
    }

    public void removeIp(final IpMapping ip) {
        unused.remove(ip);
        allIps.remove(ip.getName());
        if (ip.getIp() != null) {
            allIps.remove(ip.getIp());
        }
    }

    public IpMapping findIp(final String ipAddr) {
        return allIps.get(ipAddr);
    }
}
