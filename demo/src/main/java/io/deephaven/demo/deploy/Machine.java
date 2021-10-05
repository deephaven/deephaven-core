package io.deephaven.demo.deploy;

import io.deephaven.demo.NameConstants;
import io.deephaven.demo.NameGen;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Objects;

/**
 * DhNode:
 * <p>
 * <p>
 * Created by James X. Nelson (James@WeTheInter.net) on 20/09/2021 @ 5:13 p.m..
 */
public class Machine {
    private String host;
    private String domainName;
    private String ip;
    private boolean sshIsReady;
    private boolean controller;
    private String machineType;
    private String diskSize;
    private String diskType;
    private boolean snapshotCreate;
    private boolean inUse;
    private long expiry;
    private boolean online;
    private DomainMapping domainInUse;
    private final LinkedList<DomainMapping> allDomains = new LinkedList<>();

    public Machine() {
    }
    public Machine(final String host) {
        this.host = host;
    }

    public String getHost() {
        return host;
    }

    public void setHost(final String host) {
        this.host = host;
    }

    public String getDomainName() {
        return domainInUse == null ? domainName == null ? getHost() + "." + NameConstants.DOMAIN : domainName : domainInUse.getDomainQualified();
    }

    public void setDomainName(final String domainName) {
        this.domainName = domainName;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(final String ip) {
        this.ip = ip == null ? null : ip.trim();
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
        return Objects.hash(host);
    }

    @Override
    public String toString() {
        return "Machine{" +
                getDomainName() + "@ip: " + ip +
                '}';
    }

    public boolean isInUse() {
        return inUse;
    }

    public void setInUse(final boolean inUse) {
        this.inUse = inUse;
    }

    public void setExpiry(final long expiry) {
        this.expiry = expiry;
    }

    public long getExpiry() {
        return expiry;
    }

    public boolean isOnline() {
        return online;
    }

    public void setOnline(final boolean online) {
        this.online = online;
    }

    public DomainMapping getDomainInUse() {
        return domainInUse;
    }

    public DomainMapping useNewDomain(GoogleDeploymentManager manager) throws IOException, InterruptedException {
        final DomainMapping oldest;
        synchronized (allDomains) {
            oldest = this.allDomains.pollFirst();
        }
        if (oldest == null) {
            // create a new domain...
            final String domainRoot = this.domainInUse == null ? NameConstants.DOMAIN : domainInUse.getDomainRoot();
            DomainMapping domain = new DomainMapping(NameGen.newName(), domainRoot);
            setDomainInUse(domain);
            manager.dns().tx(tx->tx.addRecord(domain, getIp()));
            return domain;
        }
        return oldest;
    }

    public void addAvailableDomain(DomainMapping available) {
        synchronized (this.allDomains) {
            this.allDomains.add(available);
        }
    }

    public void setDomainInUse(final DomainMapping domainInUse) {
        this.domainInUse = domainInUse;
        synchronized (allDomains) {
            if (!this.allDomains.contains(domainInUse)) {
                this.allDomains.add(domainInUse);
            }
        }
    }

    public DomainMapping domain() {
        return domainInUse == null ? new DomainMapping(host, getDomainName().replace(host + ".", "")) : domainInUse;
    }
}
