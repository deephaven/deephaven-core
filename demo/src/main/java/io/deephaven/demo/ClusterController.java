package io.deephaven.demo;

import io.deephaven.demo.deploy.*;
import io.smallrye.common.constraint.NotNull;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.mutiny.core.Vertx;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.jboss.logging.Logger;

import java.io.File;
import java.io.IOException;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.deephaven.demo.NameConstants.*;
import static io.deephaven.demo.deploy.GoogleDeploymentManager.*;

/**
 * DomainController:
 * <p>
 * <p> The brains enforcing our cluster management policies via live machine-to-dns-to-ip mappings.
 * <p>
 * <p> This class reads in data from google, and uses that to glue together a set of IP addresses {@link IpPool},
 * <p> to DNS records {@link DnsPool}.
 * <p>
 * Created by James X. Nelson (James@WeTheInter.net) on 25/09/2021 @ 2:25 a.m..
 */
public class ClusterController {

    private static final Logger LOG = Logger.getLogger(ClusterController.class);

    private final GoogleDeploymentManager manager;
    private final IpPool ips;
    private final DomainPool domains;
    private final MachinePool machines;
    private final CountDownLatch latch;
    private final OkHttpClient client;
    private static ZoneId TZ_NY = ZoneOffset.of("-6");
    private static LocalTime BIZ_START = LocalTime.of(6, 0);
    // We are using NY timezone, but want to cover all N.A. business hours, so our end is 9pm NY
    private static LocalTime BIZ_END = LocalTime.of(21, 0);

    public ClusterController() {
        this(new GoogleDeploymentManager("/tmp"));
    }
    public ClusterController(@NotNull final GoogleDeploymentManager manager) {
        this.manager = manager;
        this.client = new OkHttpClient();
        latch = new CountDownLatch(4);
        if (manager == null) {
            throw new NullPointerException("Manager cannot be null");
        }
        this.ips = new IpPool();
        this.domains = new DomainPool();
        this.machines = new MachinePool();

        setTimer("Load Unused IPs", this::loadIpsUnused);
        setTimer("Load Machines", this::loadMachines);
        setTimer("Load Used IPs", this::loadIpsUsed);
        setTimer("Load Domains", this::loadDomains);
        setTimer("Wait until loaded", ()->{
            waitUntilReady();
            // a little extra delay: give user a chance to request a machine before we possibly clean it up!
            setTimer("Refresh state", ()-> {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                checkState();
                monitorLoop();
            });
        });
    }

    public static void setTimer(String name, Runnable r) {
        new Thread(name) {
            @Override
            public void run() {
                try {
                    r.run();
                } catch (Throwable t) {
                    LOG.error("Unexpected error occurred running " + name, t);
                    throw t;
                }
            }
            {
                setDaemon(true);
            }
        }.start();
    }

    private void monitorLoop() {
        final Vertx vx = Vertx.vertx();
        vx.setTimer(TimeUnit.MINUTES.toMillis(1), t->{
            setTimer("Monitor Loop", ()->{
                try {
                    checkState();
                } finally {
                    monitorLoop();
                }
            });
        });
    }

    private synchronized void checkState() {
        // check for machines that have exceeded their limit, reboot them, and then put them back in the usable pool
        Set<Machine> runningMachines = new ConcurrentHashSet<>();
        Set<Machine> availableMachines = new ConcurrentHashSet<>();
        Set<Machine> offlineMachines = new ConcurrentHashSet<>();
        Set<Machine> usedMachines = new ConcurrentHashSet<>();
        machines.getAllMachines().forEach(machine -> {
            if (machine.isOnline()) {
                runningMachines.add(machine);
                if (machine.isInUse()) {
                    usedMachines.add(machine);
                } else {
                    availableMachines.add(machine);
                }
            } else {
                machine.setInUse(false);
                offlineMachines.add(machine);
            }
            if (machine.isInUse()) {
                if (machine.getExpiry() > 0 && machine.getExpiry() < System.currentTimeMillis()) {
                    // machine is past expiry... lets turn this box off.

                    turnOff(machine);
                }
            }
        });
        int numRunning = runningMachines.size();
        int poolSize = getPoolSize();
        int numBuffer = getPoolBuffer();
        LOG.info("Done checking state:\n" +
                runningMachines.size() + " running machines\n" +
                offlineMachines.size() + " offline machines\n" +
                usedMachines.size() + " used machines\n" +
                availableMachines.size() + " available machines\n" +
                poolSize + " or more running machines desired\n" +
                numBuffer + " available machines desired\n");
        if (numRunning < poolSize){
            // not enough nodes are running... turn some on!
            for (Machine next : offlineMachines) {
                if (!next.isOnline()) {
                    numRunning++;
                    next.setOnline(true);
                    // purposely don't parallelize: google will bark at us if we scale up too fast
                    try {
                        manager.turnOn(next);
                        availableMachines.add(next);
                    } catch (IOException | InterruptedException e) {
                        LOG.error("Error turning on machine " + next, e);
                    }
                    if (numRunning >= poolSize) {
                        break;
                    }
                }
            }
            // if fewer than needed nodes running, turn things on
            // if we don't have enough machines, add however many machines we need to fill pool
            while (numRunning < poolSize) {
                LOG.warn("Only " + numRunning + " machines, but want " + poolSize + "; adding a new machine");
                numRunning++;
                final Machine newMachine = requestMachine(NameGen.newName(), false);
                availableMachines.add(newMachine);
                runningMachines.add(newMachine);
            }
        }
        if (availableMachines.size() < numBuffer) {
            while (availableMachines.size() < getPoolBuffer()) {
                LOG.warn("Only " + availableMachines.size() + " machines are running and unused, but want " + getPoolBuffer() + " available machines; adding a new machine");
                final Machine newMachine = requestMachine(NameGen.newName(), false);
                availableMachines.add(newMachine);
                runningMachines.add(newMachine);
            }
        } else if (numRunning > poolSize) {
            LOG.info(numRunning + " running machines > " + poolSize + " maximum pool size; trying to shut down extra machines");
            // if more than needed nodes are running, turn off any nodes not servicing clients
            for (Machine next : runningMachines) {
                if (!next.isInUse()) {
                    // leave a buffer of unused machines running, even if we've exceeded pool size
                    if (numBuffer--<=0) {
                        LOG.info("Turning off unneeded machine " + next.getHost());
                        numRunning--;
                        turnOff(next);
                        if (numRunning <= poolSize) {
                            break;
                        }
                    }
                }
            }

        }

    }

    private void turnOff(final Machine machine) {
        // get off the fork-join pool to do IO:
        machine.setOnline(false);
        setTimer("Decommission " + machine.getHost(), ()-> {
            try {
                // TODO: ping the machine and have a procedure that warns the user this is going to happen:
                //  also, we should be hooking up a new DNS name to the machine and deleting the one that is in use
                gcloud("instances", "stop", "-q", machine.getHost());
                manager.replaceDNS(machine);
                machine.setInUse(false);
                if (machines.needsMoreMachines(getPoolBuffer(), getPoolSize())) {
                    requestMachine();
                }
            } catch (IOException | InterruptedException e) {
                System.err.println("Error trying to restart " + machine.getHost());
                e.printStackTrace();
            }
        });
    }

    private int getPoolBuffer() {
        return 5;
    }

    private int getPoolSize() {
        String poolSize = System.getenv("POOL_SIZE");
        if (poolSize != null) {
            return Integer.parseInt(poolSize);
        }
        LocalTime time = LocalTime.now(TZ_NY);
        if (time.isAfter(BIZ_START) && time.isBefore(BIZ_END)) {
            return 30;
        }
        return 5;
    }

    private void loadMachines() {
        try {
            final Execute.ExecutionResult result = gcloud(true, false, "instances", "list",
                    "--filter", "labels." + LABEL_PURPOSE + "=" + PURPOSE_WORKER,
                    "--format", "csv[box,no-heading](name,hostname,status,networkInterfaces[0].accessConfigs[0].natIP)",
                    // we'll do paging if we actually see activity this high
                    "--page-size", "500",
                    "-q"
            );
            final String[] lines = result.out.split("\n");
            if (result.out.contains("not present")) {
                // empty, no parsing...
                System.out.println("0 active workers found");
            } else {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Got active workers:");
                    LOG.trace(result.out);
                } else {
                    LOG.info("Got " + lines.length + " total workers (use trace logging to see response)");
                }
                int online = 0;
                for (String line : lines) {
                    if (line.isEmpty()) {
                        continue;
                    }
                    String[] bits = line.split(",");
                    Machine mach = new Machine(bits[0]);
                    // nasty chunk of code I'm uncommenting whenever I want to nuke all workers to use a new worker image... needs a main() somewhere
//                    setTimer("blart", ()->
//                            {
//                                try {
//                                    gcloud("instances", "delete", "-q", mach.getHost());
//                                } catch (IOException e) {
//                                    e.printStackTrace();
//                                } catch (InterruptedException e) {
//                                    e.printStackTrace();
//                                }
//                            }
//                            );
//                    if ("".length() == 0) continue;
                    if (bits[1].length() > 0) {
                        // hmmm.... we really shouldn't be using custom hostname... it's stuck to machine on creation
                        // for now, we'll just prefer a non-null DomainMapping object, if one is assigned to this instance
                        mach.setDomainName(bits[1]);
                    }
                    if (bits[2].length() > 0) {
                        mach.setOnline("RUNNING".equals(bits[2]));
                        if (mach.isOnline()) {
                            online++;
                        }
                    }
                    if (bits.length == 4 && bits[3].length() > 0) {
                        mach.setIp(bits[3]);
                    }
                    machines.addMachine(mach);
                }
                LOG.infof("Found %s running machines", online);
            }

        } catch (IOException | InterruptedException e) {
            LOG.error("Failed to get active workers", e);
        }
        latch.countDown();
    }

    private void loadDomains() {
        latch.countDown();
    }

    private void loadIpsUnused() {
        String[] ipBits = null;
        try {
            Execute.ExecutionResult result = gcloud(true, false, "addresses", "list",
                    "--filter", "region:" + REGION + " AND status:reserved",
                    "--format", "csv[box,no-heading](name,ADDRESS)",
                    "-q"
            );
            for (String ipRec : result.out.split("\\s+")) {
                if (ipRec.isEmpty()) {
                    continue;
                }
                ipBits = ipRec.split(",");
                String name = ipBits[0];
                String addr = ipBits[1];
                IpMapping ip = new IpMapping(name, addr);
                ips.addIpUnused(ip);
            }
        } catch (Exception e) {
            System.err.println("Unable to load IPs! Last seen item: " + (ipBits == null ? null : Arrays.asList(ipBits)));
            e.printStackTrace();
        }
        latch.countDown();
    }

    private void loadIpsUsed() {
        String[] ipBits = null;
        try {
            Execute.ExecutionResult result = gcloud(true, false, "addresses", "list",
                   // TODO: label our addresses so we can filter more correctly:
                    "--filter", "region:" + REGION + " AND status:in_use",
                    "--format", "csv[box,no-heading](name,ADDRESS)",
                    "-q"
            );
            for (String ipRec : result.out.split("\\s+")) {
                ipBits = ipRec.split(",");
                String name = ipBits[0];
                String addr = ipBits[1];
                if (name.startsWith("dh-") || name.startsWith("perf-")) {
                    // ignore these addresses
                    continue;
                }
                IpMapping ip = new IpMapping(name, addr);
                ips.addIpUsed(ip);
            }
        } catch (Exception e) {
            System.err.println("Unable to load IPs! Last seen item: " + (ipBits == null ? null : Arrays.asList(ipBits)));
            e.printStackTrace();
        }
        latch.countDown();
    }

    public boolean isReady() {
        return latch.getCount() == 0;
    }

    public void waitUntilReady() {
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            String msg = "Interrupted waiting for controller to read metadata; check for error logs!";
            System.err.println(msg);
            Thread.currentThread().interrupt();
            throw new RuntimeException(msg, e);
        }
    }

    public Machine requestMachine() {
        waitUntilReady();
        Optional<Machine> machine = machines.maybeGetMachine(manager);
        if (machine.isPresent()) {
            final Machine mach = machine.get();
            final IpMapping ip = ips.reserveIp(this, mach);
            moveToRunningState(mach, ip, true);
            LOG.info("Sending user to pre-existing machine " + mach);
            return mach;
        }
        // hm... we should probably send user to interstitial page immediately...
        // no need to have them wait until machine spins up to see "you gonna have to wait" screen.
        String newName = NameGen.newName();
        LOG.info("Sending user to new machine " + newName);
        return requestMachine(NameGen.newName(), true);
    }

    public Machine requestMachine(String name, boolean reserve) {
        // reserve an IP while getting a machine going.
        final IpMapping ip = ips.getUnusedIp(this);
        final Machine machine = machines.createMachine(manager, name, ip);
        moveToRunningState(machine, ip, reserve);
        return machine;
    }

    void moveToRunningState(final Machine machine, final IpMapping ip, final boolean reserve) {
        LOG.info("Moving machine " + machine.getDomainName() + " to a running state");
        if (reserve) {
            machine.setExpiry(System.currentTimeMillis() + getSessionTtl());
            machine.setInUse(true);
        }
        setTimer("Move to running state", ()->{
             Execute.ExecutionResult result;
            String myIp = machine.getIp();
            try {
                String purpose = machine.isController() ? PURPOSE_CONTROLLER :
                                 machine.isSnapshotCreate() ? PURPOSE_CREATOR :
                                PURPOSE_WORKER;
                result = gcloud(true,"instances",
                        "describe", machine.getHost(),
                        "-q",
                        "--format=csv[box,no-heading](labels." + LABEL_PURPOSE + ",networkInterfaces[0].accessConfigs[0].natIP)");
                String data = result.out.trim();
                if (!data.startsWith(purpose + ",")) {
                    LOG.warn("Instance " + machine.getHost() + " had label '" + data.split(",")[0] + "' instead of " + purpose + "; fixing....");
                    result = gcloud(true,"instances", "add-labels", machine.getHost(), "--labels=" + LABEL_PURPOSE + "=" + purpose);
                    if (result.code != 0) {
                        // check if this was a "not found" message
                        if (result.out.contains("not found")) {
                            // yikes! the machine doesn't exist... create one w/ the specified ip address
                            manager.createMachine(machine);
                            result.code = 0;
                        }
                    }
                    if (result.code != 0) {
                        final String msg = "Unable to add label to machine " + machine.getHost();
                        LOG.error(msg);
                        GoogleDeploymentManager.warnResult(result);
                        return;
                    }
                }
                if (!data.endsWith("," + myIp)) {
                    LOG.warn("Replacing " + myIp + " with " + data.split(",")[1]);
                    if (data.contains(",")) {
                        machine.setIp(data.split(",")[1]);
                    } else {
                        LOG.error("Unexpected result: " + result.out);
                    }
                }
            } catch (Exception e) {
                LOG.error("Unable to move machine to running state: " + machine + " ip: " + ip, e);
            }
            // machine is alive, make sure it has DNS!
            final Execute.ExecutionResult dnsCheck = manager.checkDns(machine.getDomainName(), DNS_QUAD9);
            if (dnsCheck.code == 0 && dnsCheck.out.trim().equals(myIp)) {
                // machine is alive, DNS is all good.
                LOG.trace("DNS resolved for " + machine.getDomainName());
            } else {
                LOG.warn("DNS does not resolve correctly for " + machine.getDomainName() + " @ " + machine.getIp() + "; setting up DNS records");
                // machine is NOT alive, glue on some ad-hoc DNS
                try {
                    setupDns(machine);
                } catch (Exception e) {
                    LOG.error("Unable to setup DNS for machine " + machine, e);
                }
            }

        });
    }

    private long getSessionTtl() {
        return TimeUnit.MINUTES.toMillis(45);
    }

    private void setupDns(final Machine machine) throws IOException, InterruptedException, TimeoutException {
        ClusterMap derived = new ClusterMap();
        derived.setClusterName("dns-for-" + machine.getHost());
        derived.setLocalDir(manager.getLocalDir() + File.separator + machine.getHost());
        derived.setAllNodes(Collections.singletonList(machine));
        manager.assignDns(derived);
    }

    public Collection<IpMapping> requestNewIps(int numIps) {
        final List<IpMapping> list = new ArrayList<>();

        while (numIps --> 0) {
            final IpMapping mapping = new IpMapping(NameGen.newName(), null);
            list.add(mapping);
        }
        // Filling in the IP address requires IO, so let's do that offthread...
        // IMPORTANT: code calling us may be holding a lock, so don't de-off-thread this code without checking callers of this method for locks!
        setTimer("Get or create " + list.size() + "IPs", ()-> list.parallelStream().forEach(this::getOrCreateIp));

        return Collections.unmodifiableList(list);
    }

    protected void getOrCreateIp(final IpMapping ip) {
        if (ip.getIp() == null) {
            // create a new IP w/ google.
            String name = ip.getName();
            try {
                final Execute.ExecutionResult result = gcloud(true, false, "addresses", "create",
                        name, "--region", NameConstants.REGION);
                if (result.code != 0) {
                    String msg = "Unable to create an ip address for " + ip;
                    System.err.println(msg);
                    System.err.println("stdout:");
                    System.err.println(result.out);
                    System.err.println("stderr:");
                    System.err.println(result.err);
                    throw new IllegalStateException(msg);
                }
                // address exists... parse out the value!
                System.out.println("Created address for " + ip + " :\n" + result.out);
            } catch (IOException | InterruptedException e) {
                System.err.println("Unable to create an IP address for " + ip + "; check for resource quotas / service outage?");
                e.printStackTrace();
            }
        }
    }

    public boolean isMachineReady(final Machine machine) {
        String uri = "https://" + machine.getDomainName() + "/health";
        final Request req = new Request.Builder().get()
                .url(uri)
                .build();
        final Response response;
        try {
            response = client.newCall(req).execute();
        } catch (IOException e) {
            return false;
        }
        boolean success = response.isSuccessful();
        response.close();
        return success;
    }

    public GoogleDeploymentManager getDeploymentManager() {
        return manager;
    }

}
