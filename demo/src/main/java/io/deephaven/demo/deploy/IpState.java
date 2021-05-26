package io.deephaven.demo.deploy;

/**
 * When looking for a machine, we first check Released IPs for a live /health ping (degrading to Unclaimed
 * whenever a /health ping fails).  After we check Released IPs, if we fail to reserve a machine, we will try to
 * reserve an Unclaimed IP (transforming into a Claimed state), which, when successful, we spin up / turn on a machine for our IP.
 * <p>
 * If we can't even claim an IP, we'll grab / request new Unverified IPs to immediately claim ^-^
 */
public enum IpState {
    /**
     * This IP address is a freshly created that has not even requested changed to google DNS yet.
     */
    Unverified,
    /**
     * This IP address has been setup and verified, but is not claimed by any running user.
     */
    Unclaimed,
    /**
     * This IP address is claimed by a requested user session that we are in the process of creating.
     */
    Claimed,
    /**
     * This IP address is associated with a running user session on a live machine.
     * <p>
     * <p> Requires waiting until the live machine answers a /health ping.
     */
    Running,
    /**
     * This IP address is no longer used by a client, and has the highest chances to already have a running machine.
     * <p>
     * <p> We'll check Released IPs before Unclaimed IPs before Unverified IPs before giving up ^-^
     */
    Released
}
