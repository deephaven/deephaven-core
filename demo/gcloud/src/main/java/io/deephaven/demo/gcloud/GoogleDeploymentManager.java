package io.deephaven.demo.gcloud;

import com.google.common.io.CharSink;
import com.google.common.io.Files;
import io.deephaven.demo.api.*;
import io.deephaven.demo.manager.DeploymentManager;
import io.deephaven.demo.manager.Execute;
import io.deephaven.demo.manager.IClusterController;
import io.deephaven.demo.manager.NameGen;
import org.apache.commons.io.FileUtils;
import org.jboss.logging.Logger;

import java.io.*;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.demo.manager.Execute.execute;
import static io.deephaven.demo.manager.NameConstants.*;

/**
 * The first (maybe only) DeploymentManager implementation will be using google cloud.
 */
public class GoogleDeploymentManager implements DeploymentManager {

    private static final Logger LOG = Logger.getLogger(GoogleDeploymentManager.class);

    // the deephaven.app NS record points to this project, so we have to do DNS there for now.
    // the project we _should_ be using is below, which we can use after updating NS, or using a newer, shorter domain
    static final String DH_INTERNAL_PROJECT = "deephaven-oss";
    static final String DH_INTERNAL_DNS_ZONE = "deephaven-app";
    /**
     * When we first create a VM, we'll wait until google DNS, the first DNS service that will know about our gcloud cluster,
     * has a live record of our route.  The default is a public DNS server, {@link #DNS_QUAD9}
     */
    public static final String DNS_GOOGLE = "8.8.8.8";
    /**
     * A public, privacy-conscious global dns server that is reasonably fast.  This is the default DNS server we check with,
     * so any attempts to wait until an IP address is reachable will have a reasonable parity with random-client-from-anywhere.
     */
    public static final String DNS_QUAD9 = "9.9.9.9";
    private final String localDir;
    private final IpPool ips;
    private final DomainPool domains;
    private final AtomicInteger pendingIps = new AtomicInteger(0);

    boolean createdNewMachine;

    private final GoogleDnsManager dns;
    private String baseImageName;

    public static IpMapping findByIp(final IpPool ips, final String ipAddr) {
        IpMapping ip = ips.findIp(ipAddr);
        if (ip == null) {
            if ("false".equals(System.getenv("ALLOW_ADDRESS_LOOKUP"))) {
                return null;
            }
            final Execute.ExecutionResult ipLookup;
            try {
                ipLookup = GoogleDeploymentManager.gcloud(true, false,
                        "addresses", "list", "-q",
                        "--filter", "address = " + ipAddr,
                        "--format", "csv[box,no-heading](NAME)");
                if (ipLookup.code == 0) {
                    LOG.infof("FOUND MISSING IP ADDRESS %s from %s", ipLookup, ipAddr);
                    String name = ipLookup.out.trim();
                    if (name.isEmpty()) {
                    } else {
                        return ips.updateOrCreate(name, ipAddr);
                    }
                }
            } catch (IOException | InterruptedException e) {
                LOG.errorf("Could not lookup ip %s");
            }
        }
        return ip;
    }

    @Override
    public GoogleDnsManager getDns() {
        return dns;
    }

    @Override
    public boolean hasMachine(final String workerName) throws IOException, InterruptedException {
        final Execute.ExecutionResult res = gcloudQuiet(true, false,
                "instances", "list",
                "--filter=(NAME <= " + workerName + " AND NAME >= " + workerName + ")",
                "--format=value(NAME)");
        return workerName.equals(res.out.trim());
    }

    @Override
    public String guessRealIp(final String name) {
        try {
            return getGcloudIp(name, name + "." + DOMAIN);
        } catch (IOException | InterruptedException e) {
            return null;
        }
    }

    @Override
    public IpMapping findIp(final String ipAddr) {
        return findByIp(ips, ipAddr);
    }

    static String getDnsZone() {
        String zone = System.getenv("DH_GOOGLE_DNS_ZONE");
        if (zone == null) {
            zone = DH_INTERNAL_DNS_ZONE;
        }
        return zone;
    }
    public static String getGoogleProject() {
        String envProj = System.getenv("DH_GOOGLE_PROJECT");
        if (envProj == null) {
            envProj = DH_INTERNAL_PROJECT;
        }
        return envProj;
    }

    public static String getGoogleZone() {
        String envZone = System.getenv("DH_GOOGLE_ZONE");
        if (envZone == null) {
            envZone = "us-central1-f";
        }
        return envZone;
    }

    static String getLargeDiskId() {
        String largeDisk = System.getenv("DH_LARGE_DISK");
        if (largeDisk == null) {
            largeDisk = "demo-data-b";
        }
        return largeDisk;
    }

    static final long DNS_CHECK_MILLIS = 360_000; // wait up to five minutes for DNS to resolve

    public GoogleDeploymentManager(String localDir) {
        this.localDir = localDir;
        dns = new GoogleDnsManager(new File(localDir, "dns"));
        ips = new IpPool();
        domains = new DomainPool();
        try {
            concatScripts(localDir, "prepare-base.sh",
                    "script-header.sh",
                    "VERSION",
                    "setup-docker.sh",
                    "finish-setup.sh");
            concatScripts(localDir, "prepare-worker.sh",
                    "script-header.sh",
                    "VERSION",
                    "setup-docker.sh",
                    "get-credentials.sh",
                    "gen-certs.sh",
                    "prepare-worker.sh",
                    "pull-images.sh",
                    "systemd-enable-dh.sh",
                    "finish-setup.sh");
            concatScripts(localDir, "prepare-controller.sh",
                    "script-header.sh",
                    "VERSION",
                    "setup-docker.sh",
                    "get-credentials.sh",
                    "gen-certs.sh",
                    "prepare-controller.sh",
                    "pull-images.sh",
                    "systemd-enable-dh.sh",
                    "finish-setup.sh");
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to create script in " + localDir, e);
        }
    }

    @Override
    public void assignDns(final IClusterController ctrl, Stream<Machine> nodes) {

        // first, query if a DNS record already exists.
        dns.tx(tx -> {
            nodes.collect(Collectors.toList()).forEach(node -> {
                IpMapping nodeIp = node.getIp();
                DomainMapping oldDomain = node.domain();
                final DomainMapping oldIpDomain = nodeIp.getCurrentDomain();
                DomainMapping mapping = oldDomain == null ? node.useNewDomain(ctrl) : oldDomain;
                while (DOMAIN.equals(node.domain().getDomainQualified())) {
                    new Exception("INCORRECTLY ASSIGNED DOMAIN ROOT TO NODE " + node +
                            "\nOld domain: " + oldDomain +
                            "\nOld IP domain: " + oldIpDomain).printStackTrace();
                    node.getIp().setDomain(null);
                    mapping = node.useNewDomain(ctrl);
                }
                LOG.infof("Ensuring node %s has domain name %s", node.getHost(), node.domain());
                try {
                    String resolved = getDnsIp(node);
                    if (nodeIp.getIp() == null) {
                        // if the IpMapping's actual IP is not known yet, wait for any pending IP-related operations to complete
                        ctrl.waitUntilIpsCreated();
                    }
                    // if the record exists, make sure it matches the expected IP address.
                    if (resolved != null && resolved.equals(nodeIp.getIp())) {
                        // record exists and is correct, do nothing for this node.
                        return;
                    }
                    // The IP address has changed, we need to remove the old one and create a new one.
                    // remove new, create, done below
                    if (resolved != null && !resolved.isEmpty()) {
                        LOG.warnf("Removing machine %s's invalid DNS record pointing to %s instead of %s",
                                node.toStringShort(), resolved, nodeIp.getIp());
                        tx.removeRecord(mapping, resolved);
                    }
                } catch(UnknownHostException ignored) {
                    // no dns record exists.  Add one to our DNS transaction (opening a new one if not already in progress)
                } catch (IOException | InterruptedException e) {
                    LOG.errorf(e, "IO error trying the get IP address for %s", node.getDomainName());
                }
                try {
                    LOG.warnf("Adding DNS for machine %s's @ %s",
                            node.toStringShort(), nodeIp.getIp());
                    tx.addRecord(mapping, nodeIp.getIp());
                } catch (IOException | InterruptedException e) {
                    LOG.error("Unknown error trying to add dns entry for " + node.getHost(), e);
                }
            });
        });
    }

    @Override
    public void createMachine(Machine node, final IpPool ips) throws IOException, InterruptedException {
        // first, query if the machine already exists.
        boolean exists = checkExists(node);
        if (exists) {
            turnOn(node);
        } else {
            if (!createNew(node, ips)) {
                throw new IllegalStateException("Machine " + node.getHost() + " (" + node.getDomainName() + ") does not exist, and createNew() failed to make the machine.");
            }
        }
        node.setOnline(true);

        if (node.getIp() == null) {
            try {
                String ip = getGcloudIp(node);
                IpMapping ipMap = findByIp(ips, ip);
                node.setIp(ipMap);
                LOG.infof("Looked up ip mapping %s for machine %s with gcloud ip %s", ipMap, node, ip);
            } catch(IOException | InterruptedException ignored){}
        }

        // We should probably translate this into a Machine object, and stuff some state in there,
        // so we know if we should try to update DNS (and warn caller that they may have issues w/
        // cached DNS resolution making machine-name-reuse volatile.
        // Automated test systems can alter /etc/hosts, or otherwise apply sane DNS.

        // We are purposely NOT using gcloud deployments deployment-manager,
        // as we want to simulate a "bare metal" experience,
        // so we don't accidentally rely on any kind of deployment-manager magic.
    }

    @Override
    public void destroyCluster(Collection<Machine> allNodes, String diskPrefix) throws IOException {
        // delete the vms, snapshots and dns records.
        LOG.warn("Destroying node: " + allNodes.stream().map(Machine::getHost).collect(Collectors.joining(" ")));
        LOG.info("\n\nYou may see some errors below about missing resources like snapshots or disks.\n" +
"Ignore them, unless you don't see the \"Done cleanup\" message, below.\n\n");
        dns.tx(tx -> {
            allNodes.parallelStream().forEach (node -> {
                try {
                    String ip = node.getIpAddress();
                    if (ip == null) {
                        ip = getGcloudIp(node);
                    }
                    tx.removeRecord(node.domain(), ip);
                } catch (IOException | InterruptedException e) {
                    LOG.error("Unknown error deleting dns entry for " + node.getHost() + " @ " + node.getIp(), e);
                    return;
                }
                try {
                    node.setDestroyed(true);
                    gcloud(false, "instances", "delete", "-q", node.getHost());
                } catch (IOException | InterruptedException e) {
                    System.err.println("Unknown error deleting instance " + node.getHost());
                    e.printStackTrace();
                }
            });
        });
        final File dnsDir = dns.dnsDir();
        long maxWait = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < maxWait) {
            if (dnsDir.exists()) {
                LOG.infof("Waiting for DNS transaction to complete in %s", dnsDir);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    break;
                }
            } else {
                break;
            }
        }
        if (System.currentTimeMillis() >= maxWait) {
            LOG.errorf("DNS transaction did in directory %s not complete within 30s", dnsDir);
        }
        FileUtils.deleteDirectory(new File(localDir));
        LOG.info("\n\nDone cleanup.  You may resume taking errors seriously.\n\n");
    }

    @Override
    public void createSnapshot(String snapshotName, ClusterMap map, boolean forceCreate, String diskPrefix) throws IOException, InterruptedException {
        if (forceCreate || needsSnapshot(snapshotName, map)) {
            List<String> snapshotArgs = new ArrayList<>(Arrays.asList("gcloud", "compute", "disks", "snapshot", "--zone", getGoogleZone()));
            List<String> snapshotNames = new ArrayList<>();
            map.getAllNodes().forEach(node -> {
                // on new machines, disk name matches node.host; on old ones, it has a "disk-" prefix
                snapshotArgs.add(diskPrefix + node.getHost());
                snapshotNames.add(node.getHost() + "-" + snapshotName);
            });
            snapshotArgs.add("--snapshot-names");
            snapshotArgs.add(String.join(",", snapshotNames));
            Execute.ExecutionResult result = execute(snapshotArgs);
            if (result.code != 0) {
                if (!result.err.contains("already exists")) {
                    throw new IllegalStateException("Fatal error trying to create snapshots of $map.clusterName");
                }
                List<String> deleteArgs = new ArrayList<>(Arrays.asList("gcloud", "compute", "snapshots", "delete", "--quiet"));
                map.getAllNodes().forEach(node ->
                    deleteArgs.add(node.getHost() + "-" + snapshotName)
                );
                execute(deleteArgs);
                result = execute(snapshotArgs);
            }
            if (result.code != 0) {
                throw new IllegalStateException("Fatal error trying to recreate snapshots of $map.clusterName");
            }
        }
    }

    @Override
    public void restoreSnapshot(String snapshotName, ClusterMap map, boolean restart, String diskPrefix) {
        if (createdNewMachine) {
            System.out.println("We just created machines for " + map.getClusterName() + "; skipping rollback request");
            return;
        }
        System.out.println("Performing VM snapshot rollback to snapshot " + snapshotName);
        map.getAllNodes().parallelStream().forEach(
                node -> {
                String snap = node.getHost() + "-" + snapshotName;
                String diskName = diskPrefix + node.getHost();
                System.out.println("Rolling back " + node.getHost() + " to " + snap);
                    try {
                        gcloud(false, "instances", "stop", node.getHost());
                    } catch (IOException | InterruptedException e) {
                        System.err.println("Unknown error stopping instance " + node.getHost());
                        e.printStackTrace();
                    }
                    long timeout = System.currentTimeMillis() + 30_000;
                while (true) {
                    try {
                        if (execute( Arrays.asList("gcloud", "compute", "instances", "list",
                                "--project", getGoogleProject(),
                                "--filter=(name <= " + node.getHost() + " AND name >= " + node.getHost() + ")")
                        ).out.contains("TERMINATED"))
                        break;
                    } catch (IOException | InterruptedException e) {
                        System.err.println("Received error waiting for " + node.getHost() + " to reach a terminated state");
                        e.printStackTrace();
                    }
                    System.out.println("Waiting for " + node.getHost() + " to report it is stopped");
            if (System.currentTimeMillis() > timeout) {
                throw new IllegalStateException("Waited 30 seconds, but " + node.getHost() + " does not report a TERMINATED status running gcloud compute instances list --filter=name=($node.host)");
            }
        }
        try {
            gcloud(false, "instances", "detach-disk", node.getHost(), "--disk", diskName);
        } catch(Exception e) {
            System.err.println("Unable to detach and disks, perhaps machine was left in inconsistent state?");
            e.printStackTrace();
        }
        try {
            gcloud(false, "disks", "delete", diskName, "-q");
        } catch(Exception e) {
            System.err.println("Unable to delete old disks, perhaps machine was left in inconsistent state?");
            e.printStackTrace();
        }
                    try {
                        gcloud(false, "disks", "create", diskName, "--source-snapshot", snap);
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                    try {
                        gcloud(false, "instances", "attach-disk", "--boot", node.getHost(), "--disk", diskName);
                    } catch (IOException | InterruptedException e) {
                        System.err.println("Unknown error attaching boot disk " + diskName + " to " + node.getHost());
                        e.printStackTrace();
                    }
                    if (restart) {
                        try {
                            gcloud(false, "instances", "start", node.getHost());
                        } catch (IOException | InterruptedException e) {
                            System.err.println("Unknown error starting instance " + node.getHost());
                            e.printStackTrace();
                        }
                    }
        });
    }

    @Override
    public void waitForSsh(Machine node) {
        // note: the TTL for our DNS records is 300s, or 5 minutes.  Thus, we'll wait at least 9 minutes the update to propagate
        waitForSsh(node, TimeUnit.MINUTES.toMillis(3), TimeUnit.MINUTES.toMillis(9));
    }

    public void waitForSsh(Machine node, long rebootTimeoutMillis, long totalTimeoutMillis) {
        if (node.isSshIsReady()) {
            return;
        }
        LOG.info("Waiting for ssh to respond on " + node.getDomainName());
        // now, wait until the instance is responding to ssh.
        long minutes = TimeUnit.MILLISECONDS.toMinutes(totalTimeoutMillis);
        final long startMillis = System.currentTimeMillis();
        final long endMillis = startMillis + totalTimeoutMillis;
        Throwable last_fail;
        boolean printOnce = true;
        boolean rebootLeft = rebootTimeoutMillis > 0;
        int delay = 1000;
        while (true) {
            try {
                Execute.ExecutionResult result;
                if (rebootLeft && System.currentTimeMillis() - startMillis > rebootTimeoutMillis) {
                    rebootLeft = false;
                    System.out.println("\nWaited more than " + TimeUnit.MILLISECONDS.toSeconds(rebootTimeoutMillis) + " minutes for DNS; rebooting instance " + node.getHost());
                    turnOff(node);
                    turnOn(node);
                }
                boolean allowFail = System.currentTimeMillis() < endMillis;
                // wait until we can connect to host with ssh
                result = Execute.sshQuiet( node.getDomainName(), allowFail, "echo ready");
                if (result.code != 0) {
                    if (printOnce) {
                        printOnce = false;
                        LOG.warn("ssh either not ready, or fatally misconfigured for " + node.getDomainName() + ":");
                        warnResult(result);
                        LOG.warn("We will continue to loop for " + TimeUnit.MILLISECONDS.toSeconds(totalTimeoutMillis) + " seconds");
                    }
                    throw new RuntimeException("ssh not ready yet");
                }
                break;
            } catch(Exception e) {
                last_fail = e;
                // increase latency from 1s to 5s by .1s intervals (in reality, ssh trying to connect is often slow)
                delay = Math.min(delay + 100, 5000);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    return;
                }
                System.out.print('.');
            }
            if (System.currentTimeMillis() > endMillis) {
                last_fail.printStackTrace();
                throw new IllegalStateException("Restarted instance " + node.getHost() + ", but took more than " + minutes + " minutes for ssh to work.");
            }
        }
        LOG.info("\nSsh is responding on " + node.getDomainName());
        node.setSshIsReady(true);
    }

    public static Execute.ExecutionResult gcloud(String ... args) throws IOException, InterruptedException {
        return gcloud(false, args);
    }

    public static Execute.ExecutionResult gcloud(boolean allowFail, String ... args) throws IOException, InterruptedException {
        return gcloud(allowFail, true, args);
    }
    public static Execute.ExecutionResult gcloudQuiet(boolean allowFail, boolean hasZone, String ... args) throws IOException, InterruptedException {
        Execute.quietMode.set(true);
        try {
            return gcloud(allowFail, hasZone, args);
        } finally {
            Execute.quietMode.set(false);
        }
    }
    public static Execute.ExecutionResult gcloud(boolean allowFail, boolean hasZone, String ... args) throws IOException, InterruptedException {
        List<String> all = new ArrayList<>();
        all.add("gcloud");
        all.add("compute");
        all.addAll(Arrays.asList(args));
        if (hasZone) {
            all.add("--zone");
            all.add(getGoogleZone());
        }
        all.add("--project");
        all.add(getGoogleProject());
        return allowFail
                ? execute(all)
                : Execute.executeNoFail(all);
    }

    @Override
    public Collection<String> findMissingSnapshots(String snapshotName, ClusterMap map) {
        return map.getAllNodes().parallelStream().filter(it->{
                String snapName = it.getHost() + "-" + snapshotName;
            Execute.ExecutionResult result;
            try {
                result = execute(
                "gcloud", "compute", "snapshots", "list", "--project", getGoogleProject(), "--filter", "name <= " + snapName + " AND name >= " + snapName
                );
            } catch (IOException | InterruptedException e) {
                throw new IllegalStateException("Unhandled exception checking for snapshots", e);
            }
            if (result.code != 0) {
            throw new IllegalStateException("Fatal error trying to create snapshots of $map.clusterName");
        }
        return result.out.isEmpty();
        }).map( it -> it.getHost() + "-" + snapshotName).collect(Collectors.toList());
    }

    boolean checkExists(Machine machine) throws IOException, InterruptedException {

        Execute.ExecutionResult result = execute(
                "gcloud", "compute", "instances", "list", "--project", getGoogleProject(), "--filter", "(name <= " + machine.getHost() + " AND name >= " + machine.getHost() + ")"
         );
        if (result.code != 0) {
            throw new IllegalStateException("Fatal error trying to check if " + machine.getHost() + " exists");
        }
        return result.out.length() > 0 && !"Listed 0 items.\n".equals(result.out);
    }

    /**
     * Creates a new google cloud machine from a given DhNode configuration object.
     * <p><p>
     * If you wish to create a new machine in your shell, some bash that creates machines in a similar way would be:
     * <code><pre>

     # You can choose to either set hosts= here to a space-separated list, and copy this whole while loop / code block
     hosts="vm-name-1 vm-name-2"
     while read -r host || [ -n "$host" ]; do
     # ...or, you can set a host= variable here, and just copy below this comment, and up to the "dns transaction execute" part
     #host=vm-name


     PROJECT_ID=illumon-eng-170715

     # create machine
     gcloud compute instances create $host \
     --image centos-7-v20200910 \
     --image-project centos-cloud \
     --zone us-central1-f \
     --boot-disk-size 20G \
     --boot-disk-type pd-standard \
     --boot-disk-device-name $host \
     --machine-type n1-standard-4 \
     --no-address --tags=no-ip

     # find ip
     ip_addr="$(gcloud compute instances describe $host --format "value(networkInterfaces[0].networkIP)")"


     # setup dns
     dns_val=${host}.int.illumon.com.
     if [ ! -f transaction.yaml ]; then
     gcloud dns record-sets transaction start --zone=internal-illumon
     fi
     gcloud dns --project "${PROJECT_ID}" record-sets transaction add "$ip_addr" \
     --name="$dns_val" --ttl=300 --type=A --zone=internal-illumon
     # technically we could call 'dns transaction execute' outside the while/done loop, but you'll get first dns resolved faster this way
     gcloud dns --project "${PROJECT_ID}" record-sets transaction execute --zone=internal-illumon



     # Do not copy this if you are just setting up a single host at a time
     done < <(echo $hosts)


     </pre></code>
     *
     * @param machine An instance of DhNode which describes the machine we are about to create.
     * @param ips
     * @return true if we successfully created the machine.
     */
    boolean createNew(Machine machine, final IpPool ips) throws IOException, InterruptedException {
        // create a new, empty centos 7 / ubuntu 20.04 machine.
        // in the future, we'll add snapshots or source images to duplicate effort,
        // but for now our goal is to deliver a complete list of all operations needed
        // to transform a clean centos 7 / ubuntu box into a deephaven installation,
        // so we're purposely avoiding a "free lunch" from our polluted BHS images.

        createdNewMachine = true;
        // create a command list w/ common cli arguments
        List<String> cmds = new ArrayList<>(Arrays.asList(
                "gcloud", "compute", "instances", "create", machine.getHost(),
                "--zone", getGoogleZone(),
                "--project", getGoogleProject(),
                "--boot-disk-size", machine.getDiskSize(),
                "--boot-disk-type", machine.getDiskType(),
                "--boot-disk-device-name", machine.getHost(),
                // don't use the current DNS name, we want the machine hostname to match the actual name of machine
                "--hostname=" + machine.getHost() + "." + DOMAIN,
                "--machine-type", machine.getMachineType()
        ));
        String extraLabel = "";
        if (machine.isNoStableIP()) {
            // don't use the --no-address flag, that gets us no-external-IP. We just want an ephemeral, one-shot throwaway IP
            LOG.info("Machine " + machine.getHost() + " will get a new ephemeral IP address");
        } else {
            IpMapping ip = machine.getIp();
            // TODO Need to test that this ip is correct and not in use
            if (ip == null) {
                throw new NullPointerException("Cannot create a machine with a null IpMapping; bad machine: " + machine);
            }
            cmds.add("--address");
            cmds.add(ip.getName());
            LOG.info("Giving machine " + machine.getHost() + " the IP address " + ip);
            extraLabel =  ","  + LABEL_IP_NAME + "=" + ip.getName();
            final DomainMapping currentDomain = ip.getCurrentDomain();
            ips.reserveIp(this, machine);
            if (currentDomain != null) {
                // set some more extraLabel for the domain
                extraLabel += "," + LABEL_DOMAIN + "=" + currentDomain.getName();
            }
        }
        boolean isBase =  getBaseImageName().equals(machine.getHost());
        final String simpleType = machine.isController() ? "controller" : isBase ? "base" : "worker";

        if (!machine.isController()) {
            // non-controller machines attach the demo-data disk, so we can mount it into worker container
            // note: it seems we can freely multi-attach disk after machine is created, but can't add disk to multiple machines when creating machines.
//            cmds.add("--disk=device-name=large-data,mode=ro,name=" + getLargeDiskId() + ",scope=zonal");
        }
        // apply node-role specific cli arguments
        if (machine.isSnapshotCreate()) {
            cmds.add("--labels=" +
                    LABEL_PURPOSE + "=" + (machine.isController() ? PURPOSE_CREATOR_CONTROLLER : PURPOSE_CREATOR_WORKER)
                    + extraLabel);
            cmds.add("--tags=dh-demo,dh-creator");
            cmds.add("--service-account");
            cmds.add("dh-controller@" + getGoogleProject() + ".iam.gserviceaccount.com");
            // only the snapshot setup machine needs to be able to pull secrets out of kubernetes
            cmds.add("--scopes");
            cmds.add("https://www.googleapis.com/auth/cloud-platform");
            // creating snapshots, we start w/ a clean image
            addImageFlag(cmds, isBase);
            final String scriptName = "prepare-" + simpleType + ".sh";
            addStartupScript(cmds, scriptName);

        } else if (machine.isController()) {
            cmds.add("--labels=" +
                    LABEL_PURPOSE + "=" + PURPOSE_CONTROLLER
                    + extraLabel);
            cmds.add("--tags=dh-demo,dh-controller");
            cmds.add("--service-account");
            // hm... the dh-controller permissions are actually only needed by snapshotCreate machines.
            // We could reduce this, but the controller does NOT allow running any user code, so :shrug:
            cmds.add("dh-controller@" + getGoogleProject() + ".iam.gserviceaccount.com");
            // controller starts from a prepared source image
            if (machine.isUseImage()) {
                cmds.add("--image");
                cmds.add(SNAPSHOT_NAME + "-controller");
            } else {
                addImageFlag(cmds, isBase);
                final String scriptName = "prepare-controller.sh";
                addStartupScript(cmds, scriptName);
            }
            cmds.add("--scopes");
            cmds.add("https://www.googleapis.com/auth/compute,https://www.googleapis.com/auth/cloud-platform");
        } else {
            cmds.add("--labels=" +
                    LABEL_PURPOSE + "=" + PURPOSE_WORKER
                    + "," + LABEL_VERSION + "=" + VERSION_MANGLE
                    + extraLabel);
            cmds.add("--tags=dh-demo,dh-worker");
            cmds.add("--service-account");
            if (machine.isUseImage()) {
                cmds.add("dh-worker@" + getGoogleProject() + ".iam.gserviceaccount.com");
                cmds.add("--image");
                cmds.add(SNAPSHOT_NAME + "-worker");
//                cmds.add("--metadata=startup-script=while ! curl -k https://localhost:10000/health &> /dev/null; do echo 'Waiting for dh stack to come up'; done ; sudo iptables -A PREROUTING -t nat -p tcp --dport 443 -j REDIRECT --to-port 10000 ; sudo iptables -A PREROUTING -t nat -p tcp --dport 80 -j REDIRECT --to-port 10000");
            } else {
                // can't setup a worker w/o extended permissions. This should only be used for testing new worker scripts
                // TODO: have an "extended worker" service account that is able to pull docker images, but not much else...
                cmds.add("dh-controller@" + getGoogleProject() + ".iam.gserviceaccount.com");
                addImageFlag(cmds, isBase);
                cmds.add("--scopes");
                cmds.add("https://www.googleapis.com/auth/cloud-platform");
                final String scriptName = "prepare-" + simpleType + ".sh";
                addStartupScript(cmds, scriptName);
            }
        }
        Execute.ExecutionResult res = execute(cmds);

        if (res.code != 0) {
            System.err.println("Unable to create machine " + machine);
            warnResult(res);
            throw new IllegalStateException("Failed to create node " + machine.getHost());
        }
        // when a machine doesn't have a stable IP, we need to parse the ephemeral IP out of this response
        if (machine.isNoStableIP()) {
            final String realIp = getGcloudIp(machine);
            machine.getIp().setIp(realIp);
            ips.reserveIp(this, machine);
        }
        if (!machine.isController()) {
            Execute.setTimer("Attach Data Disk", ()->{
                try {
                    while (gcloudQuiet(true, true, "instances", "describe", machine.getHost(), "--format=value(name)").code != 0) {
                        Thread.sleep(1000);
                    }
                    gcloud(false,
                            "instances", "attach-disk", machine.getHost(),
                            "--disk", getLargeDiskId(), "--mode=ro", "--device-name=large-data");
                } catch (IOException | InterruptedException e) {
                    LOG.errorf(e, "Failed to attach disk %s to instance %s", getLargeDiskId(), machine.getHost());
                }
            });
        }

        return true;
    }

    private void addImageFlag(final List<String> cmds, final boolean isBase) throws IOException {
        cmds.add("--image");
        if (isBase) {
            cmds.add("ubuntu-2004-focal-v20210129");
            cmds.add("--image-project");
            cmds.add("ubuntu-os-cloud");
        } else {
            cmds.add(getBaseImageName());
        }
    }

    private void addStartupScript(final List<String> cmds, final String scriptName) throws IOException {
        if (!new File(localDir, scriptName).exists()) {
            final String prepareSnapshotPath = "/scripts/" + scriptName;
            final InputStream prepareSnapshotScript = GoogleDeploymentManager.class.getResourceAsStream(prepareSnapshotPath);
            if (prepareSnapshotScript == null) {
                System.err.println("No " + prepareSnapshotPath + " found in classloader, bailing!");
                System.exit(98);
            }
            final File scriptFile = new File(localDir, scriptName);
            final CharSink dest = Files.asCharSink(scriptFile, StandardCharsets.UTF_8);
            dest.writeFrom(new InputStreamReader(prepareSnapshotScript));
            scriptFile.setExecutable(true);
        }
        // set the startup script as the machine startup-script
        cmds.add("--metadata-from-file=startup-script=" + new File(localDir, scriptName).getAbsolutePath());
    }

    @Override
    public boolean turnOn(Machine node) throws IOException, InterruptedException {
        // Turn on a given node
        Execute.ExecutionResult res = gcloud("instances", "start", node.getHost());
        if (res.code != 0) {
            // hm... we should check if the stderr message is complaining about a machine w/o a boot disk, so we know to try a snapshot restore
            throw new IllegalStateException("Failed to turn on node " + node.getHost() + "\n" + res.err);
        }
        IpMapping nodeIp = node.getIp();
        if (nodeIp == null || nodeIp.getIp() == null) {
            // Try to parse out the external IP address of the machine
            String externalIp = "external IP is ";
            int ind = res.out.indexOf(externalIp);
            if (ind != -1) {
                String ip = res.out.substring(ind + externalIp.length()).split("\n")[0];
                if (nodeIp == null) {
                    LOG.infof("No IP for machine %s looking it up by IP %s", node.toStringShort(), ip);
                    nodeIp = findByIp(ips, ip);
                    node.setIp(nodeIp);
                } else {
                    ips.updateOrCreate(nodeIp.getName(), ip);
                }
            }
        }

        return true;
    }

    public boolean turnOff(Machine node) throws IOException, InterruptedException {
        // Turn off a given node
        Execute.ExecutionResult res = execute(
                "gcloud", "compute", "instances", "stop", node.getHost(),
                "--zone", getGoogleZone(), "--project", getGoogleProject());
        if (res.code != 0) {
            throw new IllegalStateException("Failed to turn off node " + node.getHost() + "\n" + res.err);
        }
        return true;
    }

    protected boolean needsSnapshot(String snapshotName, ClusterMap map) {
        return !findMissingSnapshots(snapshotName, map).isEmpty();
    }

    public static void warnResult(final Execute.ExecutionResult result) {
        final Throwable error;
        if (result == null) {
            error = new NullPointerException().fillInStackTrace();
            LOG.error("Calling warnResult with null result", error);
            return;
        }
        // differentiate a null pointer on the out/err field (NULL) with a non-null result of "null", which is a different error state
        String out = result.out == null ? "NULL" : result.out.trim();
        String err = result.err == null ? "NULL" : result.err.trim();
        error = new IllegalStateException("Result failed / rejected").fillInStackTrace();
        LOG.warnf(error, "Response failed (code %s)", result.code);
        if (out.isEmpty()) {
            LOG.warn("stdout: \"\"");
        } else {
            LOG.warnf("stdout: %s", out);
        }
        if (err.isEmpty()) {
            LOG.warn("stderr: \"\"");
        } else {
            LOG.warnf("stderr: %s", err);
        }

    }

    public void waitForDns(Collection<Machine> nodes, String dnsServer) throws InterruptedException, TimeoutException {
        if (dnsServer == null) {
            // if no DNS server specified, check w/ a public DNS service, as google DNS resolves faster than non-google-dns clients
            dnsServer = DNS_QUAD9;
        }
        List<Machine> changed = new ArrayList<>(nodes);
        long deadline = System.currentTimeMillis() + DNS_CHECK_MILLIS;
        System.out.println("Waiting until dns resolves for " + nodes.stream().map(Machine::getDomainName).collect(Collectors.joining(", ")) + "\n" +
"This may take a while....\n" +
"On a linux system, you _may_ want to flush your dns cache: sudo systemd-resolve --flush-caches");

        int loop = 1;
        String errLog = "";
        while (!changed.isEmpty()) {
            Machine waitFor = changed.remove(0);
            String resolvedIp = "";
            try {
                // code below explicitly uses google nameserver, to avoid any locally cached values.
                Execute.ExecutionResult res = checkDns(waitFor.getDomainName(), dnsServer);
                if (res.code == 0) {
                    resolvedIp = res.out.trim();
                    if (!resolvedIp.isEmpty()) {
                        String newErr = "Resolved " + waitFor.getDomainName() + " to " + resolvedIp + " (expected " + waitFor.getIp() + ")";
                        if (errLog.equals(newErr)) {
                            loop = dotDotDot(loop);
                        } else {
                            errLog = newErr;
                            System.out.println(newErr);
                            System.out.print("Waiting for DNS to update to expected value (" + waitFor.getIp() + ") from DNS server " + dnsServer);
                        }
                        final IpMapping ip = waitFor.getIp();
                        if (ip == null || !resolvedIp.equals(ip.getIp().trim())) {
                            Thread.sleep(1500);
                        }
                    } else {
                        loop = dotDotDot(loop);
                        // lets not hammer cpu / dns resolution
                        Thread.sleep(1500);
                    }
                    if (!res.err.isEmpty()) {
                        System.out.println("Saw error resolving dns for " + waitFor.getDomainName() + ":\n" + res.err + "\n");
                    }
                } else {
                    System.out.println("Failed to get ip: " + res.code + "\n" +
                            "stdout:" + res.out + "\nstderr:\n" + res.err);
                    resolvedIp = null;
                    // lets not hammer cpu / dns resolution
                    Thread.sleep(1500);
                }
//            } catch(UnknownHostException ignored) {
//                // causes this node to be put back on the list
//                resolvedIp = "";
            } finally {}

            if (resolvedIp == null || waitFor.getIp() == null || waitFor.getIp().getIp() == null || !resolvedIp.trim().equals(waitFor.getIp().getIp().trim())) {
                changed.add(waitFor);
            }
            if (System.currentTimeMillis() > deadline) {
                throw new TimeoutException("Waited " + DNS_CHECK_MILLIS + "ms for DNS to resolve correctly, but it failed.\n" +
                        "You may have bad cached DNS, or DNS is simply resolving very slowly;\n" +
                        "verify your machine and a different machine agree on the result of `getent hosts $waitFor.domainName`\n" +
                        "If these values disagree, please lookup how to flush DNS for your machine, or simply reboot it.\n" +
                        "Expected value: $waitFor.ip, resolved value: $resolvedIp");
            }
        }
    }

    public Execute.ExecutionResult checkDns(final String domainName, String dnsServer) {
        if (dnsServer == null || dnsServer.isEmpty()) {
            dnsServer = DNS_QUAD9;
        }
        final String bashFunc = "( host " + domainName + " " + dnsServer + " || echo failed: has address not_found ) | grep \"has address\" | head -n 1 | cut -d \" \" -f 4";
        try {
            final Execute.ExecutionResult result = Execute.bashQuiet("find_ip", bashFunc);
            if (result.out.trim().equals("not_found")) {
                result.code = 101;
            }
            return result;
        } catch (IOException | InterruptedException e) {
            String msg = "Unable to check DNS for " + domainName + " using DNS server " + dnsServer;
            e.printStackTrace();
            System.err.println(msg);
            final Execute.ExecutionResult result = new Execute.ExecutionResult(bashFunc);
            result.code = 101;
            result.out = "";
            result.err = msg;
            return result;
        }
    }

    private int dotDotDot(int loop) {
        System.out.print('.');
        System.out.flush();
        if (loop++ % 80 == 0) {
            System.out.println();
        }
        return loop;
    }

    String getDnsIp(Machine node) throws IOException, InterruptedException {
        Execute.ExecutionResult result = execute(Arrays.asList(
                "gcloud", "dns", "record-sets", "list", "--project=" + getGoogleProject(),
                "--name=" + node.getDomainName() + ".", "--type=A", "--zone=" + getDnsZone(), "--format=value(rrdatas[0])"
        ));
        if (result.code != 0 || result.out.trim().isEmpty()) {
            throw new UnknownHostException(node.getDomainName());
        }
        return result.out.trim();
    }

    String getGcloudIp(Machine node) throws IOException, InterruptedException {
        return getGcloudIp(node.getHost(), node.getDomainName());
    }

    String getGcloudIp(String nodeHost, String nodeDomain) throws IOException, InterruptedException {
        // grab the _external_ IP address
        Execute.ExecutionResult result = execute(Arrays.asList(
                "gcloud", "compute", "instances", "describe", "--project=" + getGoogleProject(),
                nodeHost, "--zone", getGoogleZone(), "--format=value(networkInterfaces[0].accessConfigs[0].natIP)"
        ));
        if (result.code != 0 || result.out.trim().isEmpty()) {
            throw new UnknownHostException(nodeDomain);
        }
        return result.out.trim();
    }

    public String getLocalDir() {
        return localDir;
    }

    /**
     * We want to throw away old domains once they've been used, so people can't revisit an old link to spy on someone's session.
     * Whenever we delete a DNS record, create a new one to take its place, so there's always a fresh address.
     * @param machine
     */
    public void replaceDNS(final IClusterController ctrl, final Machine machine) throws IOException, InterruptedException {
        dns.tx(tx -> {
            machine.useNewDomain(ctrl);
        });
    }

    public GoogleDnsManager dns() {
        return dns;
    }

    public IpPool getIpPool() {
        return ips;
    }

    private void concatScripts(final String into, final String outputFilename, final String ... scripts) throws IOException {
        final File outFile = new File(into, outputFilename);
        outFile.getParentFile().mkdirs();
        if (outFile.isFile()) {
            if (!outFile.delete()) {
                throw new IllegalStateException("Unable to delete file " + outFile);
            }
        }
        if (!outFile.createNewFile()) {
            throw new IllegalStateException("Unable to create new file: " + outFile);
        }
        LOG.info("Creating script file://" + outFile);
        try (final FileOutputStream out = new FileOutputStream(outFile, true)) {
            for (String script : scripts) {
                // Gradle sends us the version via sysprop, and we pass that along to startup script here:
                if ("VERSION".equals(script)) {
                    byte[] versionBytes = ("VERSION=" + VERSION + "\n").getBytes();
                    out.write(versionBytes, 0, versionBytes.length);
                    continue;
                }
                try (final InputStream in = GoogleDeploymentManager.class.getResourceAsStream("/scripts/" + script)) {
                    if (in == null) {
                        throw new NullPointerException("No scripts/" + script + " file");
                    }
                    int num = Math.max(4096, in.available());
                    final byte[] bytes = new byte[num];
                    for (int r;
                         (r = in.read(bytes, 0, num)) > 0;
                    ) {
                        LOG.tracef("Wrote %s bytes from scripts/%s to %s", r, script, outputFilename);
                        out.write(bytes, 0, r);
                    }
                    out.write('\n');
                }
            }
        }

    }

    public Collection<DomainMapping> getAllDNS(boolean checkOutdated, final DomainPool domains) throws IOException, InterruptedException {
        Execute.ExecutionResult result = Execute.executeQuiet(Arrays.asList(
                "gcloud", "dns", "record-sets", "list", "--project=" + getGoogleProject(),
                 "--zone=" + getDnsZone(), "--format=csv[box,no-heading](name,type,rrdatas[0])"
        ));

        Map<DomainMapping, String> valid = new ConcurrentHashMap<>();
        Map<DomainMapping, String> invalid = new ConcurrentHashMap<>();
        AtomicInteger pending = new AtomicInteger();
        if (result.code == 0) {
            for (String line : result.out.split("\n")) {
                final String[] items = line.split(",");
                if (items.length == 3) {
                    if (!"A".equals(items[1].trim())) {
                        continue;
                    }
                    if ((DOMAIN + ".").equals(items[0])) {
                        // skip the root domain
                        continue;
                    }
                    // skip any domain patterns that we probably shouldn't touch!
                    switch (items[0].split("-")[0]) {
                        case "controller":
                        case "demo":
                        case "dh":
                        case "generate":
                        case "generator":
                        case "ancestor":
                            continue;
                    }
                    String simpleName = items[0].split("[.]")[0];
                    String domainRoot = DOMAIN.equals(items[0]) ? DOMAIN :
                            items[0].replace(simpleName + ".", "");
                    if (domainRoot.endsWith(".")) {
                        domainRoot = domainRoot.substring(0, domainRoot.length() - 1);
                    }
                    DomainMapping domain = domains.getOrCreate(simpleName, domainRoot);
                    pending.incrementAndGet();

                    Execute.setTimer("Find address for " + items[2], ()->{
                        try {
                            boolean hadIp = ips.hasIp(items[2]);
                            final IpMapping ownerIp = findByIp(ips, items[2]);
                            if (!hadIp) {
                                LOG.infof("Looked up ownerIp %s for domain %s / ip %s", ownerIp, domain, items[2]);
                            }
                            if (ownerIp == null) {
                                invalid.put(domain, items[2]);
                            } else {
                                valid.put(domain, items[2]);
                                ownerIp.addDomainMapping(domain);
                            }
                        } finally {
                            pending.decrementAndGet();
                            synchronized (pending) {
                                pending.notifyAll();
                            }
                        }
                    });
                } else {
                    LOG.errorf("Malformed DNS response line: %s", line);
                }
            }
            long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2);
            while (pending.get() > 0) {
                synchronized (pending) {
                    pending.wait(1000);
                }
                if (System.currentTimeMillis() > deadline) {
                    LOG.error("Took more than 2 minutes to read DNS... something is wrong!");
                    break;
                }
            }
            LOG.infof("Found %s valid domains (use trace logging to see more)", valid.size());
            LOG.infof("Found %s invalid domains (use trace logging to see more)", invalid.size());
            if (LOG.isTraceEnabled()) {
                LOG.trace("\n\nVALID DNS RECORDS:\n" +
                        valid.keySet().stream().map(DomainMapping::toString).collect(Collectors.joining("\n")));
                LOG.trace("\n\nINVALID DNS RECORDS:\n" +
                        invalid.entrySet().stream().map(e->
                                e.getKey() + " -> " + e.getValue()).collect(Collectors.joining("\n")));
            }
            if (checkOutdated) {
                // this check is expensive, we only do it once
                // badNames are domains that have words we no longer want in a domain name.
                final Set<String> badNames = checkOutdated ? NameGen.findInvalid(valid.keySet().stream()
                        .map(DomainMapping::getName)
                        .collect(Collectors.toList())) : Collections.emptySet();
                for (Iterator<Map.Entry<DomainMapping, String>> itr = valid.entrySet().iterator();
                     itr.hasNext();) {
                    final Map.Entry<DomainMapping, String> maybe = itr.next();
                    if (badNames.contains(maybe.getKey().getName())) {
                        LOG.infof("Removing bad-named domain %s", maybe.getKey());
                        itr.remove();
                        invalid.put(maybe.getKey(), maybe.getValue());
                    }
                }
            }

            if (!invalid.isEmpty()) {
                if (LOG.isInfoEnabled()) {
                    LOG.infof("Removing %s invalid domain names: %s", invalid.size(), invalid.keySet().stream().map(DomainMapping::getDomainQualified).collect(Collectors.joining("\n")));
                }
                dns().tx(tx -> {
                    for (Map.Entry<DomainMapping, String> item : invalid.entrySet()) {
                        tx.removeRecord(item.getKey(), item.getValue());
                    }
                });
            }

        }
        LOG.info("Done checking domains");
        return valid.keySet();
    }

    public DomainPool getDomainPool() {
        return domains;
    }

    @Override
    public Collection<IpMapping> requestNewIps(int numIps) {
        final List<IpMapping> list = new ArrayList<>();

        while (numIps --> 0) {
            final IpMapping mapping = ips.updateOrCreate(NameGen.newName(), null);
            list.add(mapping);
        }
        // Filling in the IP address requires IO, so let's do that offthread...
        // IMPORTANT: code calling us may be holding a lock, so don't de-off-thread this code without checking callers of this method for locks!
        pendingIps.incrementAndGet();
        Execute.setTimer("Get or create " + list.size() + "IPs", ()-> {
            list.parallelStream().forEach(this::getOrCreateIp);
            // hmm... operations like setting up DNS _require_ this operation to be complete.
            // we should have some way to communicate this / pass on a BlockOnMe object...
            if (pendingIps.decrementAndGet() <= 0) {
                synchronized (pendingIps) {
                    pendingIps.notifyAll();
                }
            }
        });

        return Collections.unmodifiableList(list);
    }

    protected void getOrCreateIp(final IpMapping ip) {
        if (ip.getIp() == null) {
            // create a new IP w/ google.
            String name = ip.getName();
            try {
                final Execute.ExecutionResult result = gcloud(true, false, "addresses", "create",
                        name, "--region", REGION);
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
                ips.addIpUnused(ip);
            } catch (IOException | InterruptedException e) {
                System.err.println("Unable to create an IP address for " + ip + "; check for resource quotas / service outage?");
                e.printStackTrace();
            }
        }
    }

    public String getBaseImageName() {
        return baseImageName == null ? "base-" + (VERSION_MANGLE.replaceFirst("^([0-9]+[-][0-9]+).*$", "$1")) : baseImageName;
    }

    public void setBaseImageName(final String baseImageName) {
        this.baseImageName = baseImageName;
    }

    class AddLabelRequest extends GcloudApiRequest{

        private final Map<String, String> labels;
        public void addLabel(final String name, final String value, final BiConsumer<Execute.ExecutionResult, Throwable> onDone) {
            synchronized (getSynchroObject()) {
                labels.put(name, value);
                addDoneCallback(onDone);
            }
        }

        public AddLabelRequest(final String host, final String labelName, final String labelValue, final BiConsumer<Execute.ExecutionResult, Throwable> onDone) {
            super(host, onDone);
            labels = new LinkedHashMap<>();
            labels.put(labelName, labelValue);
        }

        @Override
        protected Object getSynchroObject() {
            return addLabelJobs;
        }

        @Override
        protected String[] getArgs() {
            return new String[]{
                "instances", "add-labels", getHost(),
                "--labels=" + labels.entrySet().stream()
                        .map(e -> e.getKey() + "=" + e.getValue())
                        .collect(Collectors.joining(","))
            };
        }

    }

    class RemoveLabelRequest extends GcloudApiRequest{

        private final Set<String> labels;
        public void removeLabel(final String name, final BiConsumer<Execute.ExecutionResult, Throwable> onDone) {
            synchronized (getSynchroObject()) {
                labels.add(name);
                addDoneCallback(onDone);
            }
        }

        public RemoveLabelRequest(final String host, final String labelName, final BiConsumer<Execute.ExecutionResult, Throwable> onDone) {
            super(host, onDone);
            labels = new LinkedHashSet<>();
            labels.add(labelName);
        }

        @Override
        protected String[] getArgs() {
            return new String[] {
                "instances", "remove-labels", getHost(),
                "--labels=" + String.join(",", labels)
            };
        }

        @Override
        protected Object getSynchroObject() {
            return removeLabelJobs;
        }
    }

    private final IdentityHashMap<Machine, AddLabelRequest> addLabelJobs = new IdentityHashMap<>();
    private final IdentityHashMap<Machine, RemoveLabelRequest> removeLabelJobs = new IdentityHashMap<>();

    @Override
    public void addLabel(final Machine mach, final String name, final String value, BiConsumer<Execute.ExecutionResult, Throwable> onDone) {
        AddLabelRequest job;
        final String machineString = mach.toStringShort();
        synchronized (addLabelJobs) {
            job = addLabelJobs.get(mach);
            // if the job is null, we are first, and will schedule a new job
            // if the job is already started, we will also schedule a new job.
            if (job == null || job.isStarted()) {
                job = new AddLabelRequest(mach.getHost(), name, value, onDone);
                addLabelJobs.put(mach, job);
            } else {
                // non-null, not-started job, we'll just add more labels to the request
                job.addLabel(name, value, onDone);
                // exit early, so we can move job.schedule out of the synchronized block
                return;
            }
        }
        job.schedule("Add labels to " + machineString);
    }

    @Override
    public Execute.ExecutionResult deleteMachine(final String hostName) throws IOException, InterruptedException {
        return gcloud( true, "instances", "delete", "-q", hostName);
    }

    @Override
    public void checkIpState(final IpPool ipPool, final IpMapping ip) {
        Execute.setTimer("Check IP " + ip.getName(), ()-> {
            final Execute.ExecutionResult result = GoogleDeploymentManager.gcloud(true, false, "addresses", "describe", ip.getName());
            if (result.code != 0) {
                LOG.error("IP address " + ip + " does not exist! Grep logs for QUOTA, or other address-related errors.");
                ipPool.removeIp(ip);
            }
            return "";
        });

    }

    @Override
    public void removeLabel(final Machine mach, final String name, BiConsumer<Execute.ExecutionResult, Throwable> onDone) {
        RemoveLabelRequest job;
        final String machineString = mach.toStringShort();
        synchronized (removeLabelJobs) {
            job = removeLabelJobs.get(mach);
            if (job == null || job.isStarted()) {
                job = new RemoveLabelRequest(mach.getHost(), name, onDone);
                removeLabelJobs.put(mach, job);
            } else {
                job.removeLabel(name, onDone);
                // exit early, so we can move job.schedule out of the synchronized block
                return;
            }
        }
        job.schedule("Remove labels from " + machineString);
    }

    @Override
    public Logger getLog() {
        return LOG;
    }

    @Override
    public void waitUntilIpsCreated() {
        synchronized (pendingIps) {
            while (pendingIps.get() > 0) {
                try {
                    pendingIps.wait(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.errorf("Interrupted while waiting on pendingIps to == 0 instead of " + pendingIps.get());
                    break;
                }
            }
        }
    }
}

