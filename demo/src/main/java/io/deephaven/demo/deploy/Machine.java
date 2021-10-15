package io.deephaven.demo.deploy;

import io.deephaven.demo.ClusterController;
import io.deephaven.demo.NameGen;
import io.smallrye.common.constraint.NotNull;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.Objects;

import static io.deephaven.demo.NameConstants.*;

/**
 * DhNode:
 * <p>
 * <p>
 * Created by James X. Nelson (James@WeTheInter.net) on 20/09/2021 @ 5:13 p.m..
 */
public class Machine {

    private static final Logger LOG = Logger.getLogger(Machine.class);
    static boolean useImages = true;

    private String host;
    private IpMapping ip;
    private boolean sshIsReady;
    private boolean controller;
    private String machineType;
    private String diskSize;
    private String diskType;
    private boolean snapshotCreate;
    private volatile boolean inUse;
    private volatile long expiry;
    private volatile boolean online;
    private String version;
    private volatile long mark;
    private boolean useImage;
    private boolean noStableIP;
    private boolean destroyed;

    public Machine(@NotNull final String host, IpMapping ip) {
        this.host = host;
        this.ip = ip;
        this.useImage = useImages;
        NameGen.reserveName(host);
    }

    public String getHost() {
        return host;
    }

    public void setHost(final String host) {
        this.host = host;
    }

    public String getDomainName() {
        final DomainMapping dns = domain();
        return dns == null ? getHost() + "." + DOMAIN : dns.getDomainQualified();
    }

    public void setDomainName(final String domainName) {
        this.ip.selectDomain(this, domainName);
    }

    public IpMapping getIp() {
        return ip;
    }

    public void setIp(final IpMapping ip) {
        this.ip = ip;
    }

    public boolean isSshIsReady() {
        return sshIsReady;
    }

    public void setSshIsReady(final boolean sshIsReady) {
        this.sshIsReady = sshIsReady;
    }

    public boolean isController() {
        return controller;
    }

    public void setController(final boolean controller) {
        this.controller = controller;
    }

    public String getMachineType() {
        return machineType == null ? "n2d-standard-4" : machineType;
    }

    public void setMachineType(final String machineType) {
        this.machineType = machineType;
    }

    public String getDiskSize() {
        return diskSize == null ? "200G" : diskSize;
    }

    public void setDiskSize(final String diskSize) {
        this.diskSize = diskSize;
    }

    public String getDiskType() {
        return diskType == null ? "pd-ssd" : diskType; // use pd-standard if you want to save money
    }

    public void setDiskType(final String diskType) {
        this.diskType = diskType;
    }

    public boolean isSnapshotCreate() {
        return snapshotCreate;
    }

    public void setSnapshotCreate(final boolean snapshotCreate) {
        this.snapshotCreate = snapshotCreate;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Machine machine = (Machine) o;
        return host.equals(machine.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host) ^ NameGen.getLocalHash();
    }

    public String toStringShort() {
        return getHost() + "(" + getDomainName() + ")";
    }

    @Override
    public String toString() {
        return "Machine{" +
                getHost() +" : " +
                getDomainName() + " @ " + ip +
                '}';
    }

    public boolean isInUse() {
        return inUse;
    }

    public void setInUse(final boolean inUse) {
        this.inUse = inUse;
    }

    /* Purposely not public, see caller for details */ void setExpiry(final long expiry) {
        this.expiry = expiry;
    }

    public long getExpiry() {
        return expiry;
    }

    public final boolean isOffline() {
        return !isOnline();
    }
    public boolean isOnline() {
        return online;
    }

    public void setOnline(final boolean online) {
        this.online = online;
    }

    public DomainMapping useNewDomain(ClusterController ctrl) {
        final DomainMapping previous = ip.getCurrentDomain();
        final GoogleDeploymentManager manager = ctrl.getDeploymentManager();
        final GoogleDnsManager dns = manager.dns();
        final DomainPool domainPool = manager.getDomainPool();
        dns.tx(tx -> {
            LOG.infof("Expiring ip %s for machine %s", ip, Machine.this);
            ip.expireDomain();
            DomainMapping current = ip.getCurrentDomain();
            String ctrlIp = null;
            if (previous != null) {
                ctrlIp = getIpAddressBlocking(ctrl);
                // only remove the previous record if it has changed!
                tx.removeRecord(previous, ctrlIp);
            }
            if (current == null) {
                // create a new domain...
                final String domainRoot = previous == null ? DOMAIN : previous.getDomainRoot();
                current = domainPool.getOrCreate(NameGen.newName(), domainRoot);
                ip.addDomainMapping(current);
                final DomainMapping toAdd = current;
                if (ctrlIp == null) {
                    // only block for this once, it shouldn't be changing
                    ctrlIp = getIpAddressBlocking(ctrl);
                }
                tx.addRecord(toAdd, ctrlIp);
            }
            // off-thread the "set label on machine pointing to current hostname"
            final String domainRoot = current.getDomainRoot();
            ClusterController.setTimer("Set " + getHost() + " domain to " + domain(), ()-> {
                if (!isDestroyed()) {
                    manager.addLabel(this, LABEL_DOMAIN, domain().getName());
                }
                // if our IP is running low on DNS names, we should preemptively make a few here.
                if (ip.getDomainsAvailable() < 5) {
                    LOG.infof("IP %s only has %s domains available; adding more", ip, ip.getDomainsAvailable());
                    dns.tx(t->{
                        while (ip.getDomainsAvailable() < 5) {
                            final DomainMapping domain = domainPool.getOrCreate(NameGen.newName(), domainRoot);
                            ip.addDomainMapping(domain);
                            tx.addRecord(domain, ip.getIp());
                        }
                    });
                }
            });
        });
        return ip.getCurrentDomain();
    }

    public String getIpAddressBlocking(final ClusterController ctrl) {
        if (ip != null && ip.getIp() == null) {
            ctrl.waitUntilIpsCreated();
        }
        return getIpAddress();
    }
    public String getIpAddress() {
        return ip == null ? null : ip.getIp();
    }

    public DomainMapping domain() {
        return ip.getCurrentDomain();
    }

    public void setVersion(final String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public void setMark(final long mark) {
        this.mark = mark;
    }

    public long getMark() {
        return mark;
    }

    public String getPurpose() {
        return isSnapshotCreate() ? isController() ? PURPOSE_CREATOR_CONTROLLER : PURPOSE_CREATOR_WORKER :
                isController() ? PURPOSE_CONTROLLER : PURPOSE_WORKER;

    }

    public void setUseImage(final boolean useImage) {
        this.useImage = useImage;
    }

    public boolean isUseImage() {
        return useImage;
    }

    public void setNoStableIP(final boolean noStableIP) {
        this.noStableIP = noStableIP;
    }

    public boolean isNoStableIP() {
        return noStableIP;
    }

    public boolean isDestroyed() {
        return destroyed;
    }

    public void setDestroyed(final boolean destroyed) {
        this.destroyed = destroyed;
    }
}
