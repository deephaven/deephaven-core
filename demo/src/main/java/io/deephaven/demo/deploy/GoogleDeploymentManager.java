package io.deephaven.demo.deploy;

import com.google.common.io.CharSink;
import com.google.common.io.Files;
import io.deephaven.demo.NameConstants;
import io.deephaven.demo.NameGen;
import io.vertx.core.impl.ConcurrentHashSet;
import org.apache.commons.io.FileUtils;
import org.jboss.logging.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.demo.NameConstants.*;
import static io.deephaven.demo.deploy.Execute.execute;

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
    boolean createdNewMachine;

    private final GoogleDnsManager dns;

    static String getDnsZone() {
        String zone = System.getenv("DH_GOOGLE_DNS_ZONE");
        if (zone == null) {
            zone = DH_INTERNAL_DNS_ZONE;
        }
        return zone;
    }
    static String getGoogleProject() {
        String envProj = System.getenv("DH_GOOGLE_PROJECT");
        if (envProj == null) {
            envProj = DH_INTERNAL_PROJECT;
        }
        return envProj;
    }

    static String getGoogleZone() {
        String envZone = System.getenv("DH_GOOGLE_ZONE");
        if (envZone == null) {
            envZone = "us-central1-f";
        }
        return envZone;
    }

    static final long DNS_CHECK_MILLIS = 360_000; // wait up to five minutes for DNS to resolve

    public GoogleDeploymentManager(String localDir) {
        this.localDir = localDir;
        dns = new GoogleDnsManager(new File(localDir, "dns"));
    }

    @Override
    public void assignDns(Stream<Machine> nodes) {

        // first, query if a DNS record already exists.
        List<Machine> changed = new ArrayList<>();
        Set<Machine> changedMachines = new ConcurrentHashSet<>();
        dns.tx(tx -> {
            nodes.forEach( node -> {
                String expectedIp = node.getIp();
                final DomainMapping mapping = node.getDomainInUse() == null ? new DomainMapping(node.getHost(), DOMAIN) : node.getDomainInUse();
                try {
                    String resolved = getDnsIp(node);
                    // if the record exists, make sure it matches the expected IP address.
                    if (resolved != null && resolved.equals(expectedIp)) {
                        // record exists and is correct, do nothing for this node.
                        return;
                    }
                    // The IP address has changed, we need to remove the old one and create a new one.
                    // remove new, create, done below
                    if (resolved != null && !resolved.isEmpty()) {
                        tx.removeRecord(mapping, resolved);
                    }
                } catch(UnknownHostException ignored) {
                    // no dns record exists.  Add one to our DNS transaction (opening a new one if not already in progress)
                } catch (IOException | InterruptedException e) {
                    LOG.errorf("IO error trying the get IP address for %s", node.getDomainName(), e);
                }
                changed.add(node);
                try {
                    tx.addRecord(mapping, node.getIp());
                } catch (IOException | InterruptedException e) {
                    LOG.error("Unknown error trying to add dns entry for " + node.getHost(), e);
                }
                changed.add(node);
            });
        });
    }

    @Override
    public void createMachine(Machine node) throws IOException, InterruptedException {
        // first, query if the machine already exists.
        boolean exists = checkExists(node);
        if (exists) {
            turnOn(node);
        } else {
            if (!createNew(node)) {
                throw new IllegalStateException("Machine " + node.getHost() + " (" + node.getDomainName() + ") does not exist, and createNew() failed to make the machine.");
            }
            if (node.getIp() != null && node.getIp().indexOf('.') == -1 && node.getIp().indexOf(':') != -1) {
                // we had a named IP address (normal)... null it out so we replace it w/ the resolved real/current IP
                node.setIp(null);
            }
        }
        node.setOnline(true);

        if (node.getIp() == null || node.getIp().isEmpty()) {
            try {
                String ip = getGcloudIp(node);
                node.setIp(ip);
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
        FileUtils.deleteDirectory(new File(localDir));
        dns.tx(tx -> {
            allNodes.parallelStream().forEach (node -> {
                try {
                    gcloud(false, "instances", "delete", "-q", node.getHost());
                } catch (IOException | InterruptedException e) {
                    System.err.println("Unknown error deleting instance " + node.getHost());
                    e.printStackTrace();
                    return;
                }
                try {
                    tx.removeRecord(node.domain(), node.getIp());
                } catch (IOException | InterruptedException e) {
                    LOG.error("Unknown error deleting dns entry for " + node.getHost() + " @ " + node.getIp(), e);
                    return;
                }
                List<String> deleteArgs = Arrays.asList("gcloud", "compute", "snapshots", "delete", "--quiet");
                deleteArgs.add(node.getHost() + "-clean");
                deleteArgs.add(node.getHost() + "-finished");
                try {
                    execute(deleteArgs);
                } catch (IOException | InterruptedException e) {
                    LOG.error("Unknown error deleting dns snapshots: " + deleteArgs, e);
                    return;
                }
                String diskName = diskPrefix + node.getHost();
                try {
                    gcloud(true, "disks", "delete", diskName, "--quiet");
                } catch (IOException | InterruptedException e) {
                    LOG.error("Unknown error deleting dns disk: " + diskName, e);
                }
            });
        });
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
        if (node.isSshIsReady()) {
            return;
        }
        LOG.info("Waiting for ssh to respond on " + node.getDomainName());
        // now, wait until the instance is responding to ssh.
        long minutes = 9;
        final long startMillis = System.currentTimeMillis();
        long limitMillis = startMillis + TimeUnit.MINUTES.toMillis(minutes);
        Throwable last_fail;
        boolean printOnce = true;
        boolean rebootLeft = true;
        int delay = 1000;
        while (true) {
            try {
                Execute.ExecutionResult result;
                if (rebootLeft && System.currentTimeMillis() - startMillis > TimeUnit.MINUTES.toMillis(2)) {
                    rebootLeft = false;
                    System.out.println("\nWaited more than two minutes for DNS; rebooting instance " + node.getHost());
                    turnOff(node);
                    turnOn(node);
                }
                boolean allowFail = System.currentTimeMillis() < limitMillis;
                // wait until we can connect to host with ssh
                result = Execute.sshQuiet( node.getDomainName(), allowFail, "echo ready");
                if (result.code != 0) {
                    if (printOnce) {
                        printOnce = false;
                        LOG.warn("ssh either not ready, or fatally misconfigured:");
                        warnResult(result);
                        LOG.warn("We will continue to loop for " + TimeUnit.MILLISECONDS.toSeconds(limitMillis - System.currentTimeMillis()) + " seconds");
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
            if (System.currentTimeMillis() > limitMillis) {
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
            Execute.ExecutionResult result = null;
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
            throw new IllegalStateException("Fatal error trying to check if $dhNode.host exists");
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
     * @return true if we successfully created the machine.
     */
    boolean createNew(Machine machine) throws IOException, InterruptedException {
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
                "--hostname=" + machine.getDomainName(),
                "--machine-type", machine.getMachineType()
        ));
        String ip = machine.getIp();
        if (ip != null) {
            cmds.add("--address");
            cmds.add(ip);
            LOG.info("Giving machine " + machine.getHost() + " the IP address " + ip);
        }
        // apply node-role specific cli arguments
        if (machine.isSnapshotCreate()) {
            cmds.add("--labels=" + LABEL_PURPOSE + "=" + PURPOSE_CREATOR);
            cmds.add("--tags=dh-demo,dh-creator");
            cmds.add("--service-account");
            cmds.add("dh-controller@" + getGoogleProject() + ".iam.gserviceaccount.com");
            // only the snapshot setup machine needs to be able to pull secrets out of kubernetes
            cmds.add("--scopes");
            cmds.add("https://www.googleapis.com/auth/cloud-platform");
            // creating snapshots, we start w/ a clean image
            cmds.add("--image");
            cmds.add("ubuntu-2004-focal-v20210129");
            cmds.add("--image-project");
            cmds.add("ubuntu-os-cloud");
            // stick our prepare-worker.sh script in here
            final String prepareSnapshotPath = "/scripts/prepare-" + (machine.isController() ? "controller" : "worker") + ".sh";
            final InputStream prepareSnapshotScript = GoogleDeploymentManager.class.getResourceAsStream(prepareSnapshotPath);
            if (prepareSnapshotScript == null) {
                System.err.println("No " + prepareSnapshotPath + " found in classloader, bailing!");
                System.exit(98);
            }
            final File scriptFile = new File(localDir, "prepare-worker.sh");
            final CharSink dest = Files.asCharSink(scriptFile, StandardCharsets.UTF_8);
            dest.writeFrom(new InputStreamReader(prepareSnapshotScript));
            scriptFile.setExecutable(true);
            // set the startup script as the machine startup-script
            cmds.add("--metadata-from-file=startup-script=" + scriptFile.getAbsolutePath());
        } else if (machine.isController()) {
            cmds.add("--labels=" + LABEL_PURPOSE + "=" + PURPOSE_CONTROLLER);
            cmds.add("--tags=dh-demo,dh-controller");
            cmds.add("--service-account");
            // hm... the dh-controller permissions are actually only needed by snapshotCreate machines.
            // We could reduce this, but the controller does NOT allow running any user code, so :shrug:
            cmds.add("dh-controller@" + getGoogleProject() + ".iam.gserviceaccount.com");
            // controller starts from a prepared source snapshot
            cmds.add("--source-snapshot");
            cmds.add(NameConstants.SNAPSHOT_NAME);
            cmds.add("--scopes");
            cmds.add("https://www.googleapis.com/auth/compute,https://www.googleapis.com/auth/cloud-platform");
            // TODO: use --metadata=startup-script= to change the systemd service to only run the controller, not anything else
        } else {
            cmds.add("--labels=" + LABEL_PURPOSE + "=" + PURPOSE_WORKER);
            cmds.add("--tags=dh-demo,dh-worker");
            cmds.add("--service-account");
            cmds.add("dh-worker@" + getGoogleProject() + ".iam.gserviceaccount.com");
            cmds.add("--metadata=startup-script=while ! curl -k https://localhost:10000/health &> /dev/null; do echo 'Waiting for dh stack to come up'; done ; sudo iptables -A PREROUTING -t nat -p tcp --dport 443 -j REDIRECT --to-port 10000 ; sudo iptables -A PREROUTING -t nat -p tcp --dport 80 -j REDIRECT --to-port 10000");
            cmds.add("--image");
            cmds.add(NameConstants.SNAPSHOT_NAME);
        }
        Execute.ExecutionResult res = execute(cmds);
        // TODO: use a privileged service account for setup, and then remove the service account when creating an actual worker from the snapshot

        if (res.code != 0) {
            System.err.println("Unable to create machine " + machine);
            warnResult(res);
            throw new IllegalStateException("Failed to create node " + machine.getHost());
        }
        // Parse out the IP address of this machine?
        return true;
    }

    @Override
    public boolean turnOn(Machine node) throws IOException, InterruptedException {
        // Turn on a given node
        Execute.ExecutionResult res = gcloud("instances", "start", node.getHost());
        if (res.code != 0) {
            // hm... we should check if the stderr message is complaining about a machine w/o a boot disk, so we know to try a snapshot restore
            throw new IllegalStateException("Failed to turn on node " + node.getHost() + "\n" + res.err);
        }
        // Try to parse out the external IP address of the machine
        String externalIp = "external IP is ";
        int ind = res.out.indexOf(externalIp);
        if (ind != -1) {
            String ip = res.out.substring(ind + externalIp.length()).split("\n")[0];
            node.setIp(ip);
        }

        if (node.getIp() == null || node.getIp().isEmpty()) {
            try {
                String ip = getGcloudIp(node);
                node.setIp(ip);
            } catch(IOException | InterruptedException ignored){}
        }

        return true;
    }

    boolean turnOff(Machine node) throws IOException, InterruptedException {
        // Turn off a given node
        Execute.ExecutionResult res = execute(
                "gcloud", "compute", "instances", "stop", node.getHost(),
                "--zone", "us-central1-f");
        if (res.code != 0) {
            throw new IllegalStateException("Failed to turn off node " + node.getHost() + "\n" + res.err);
        }
        return true;
    }

    protected boolean needsSnapshot(String snapshotName, ClusterMap map) {
        return !findMissingSnapshots(snapshotName, map).isEmpty();
    }

    public static void warnResult(final Execute.ExecutionResult result) {
        String out = result.out.trim();
        String err = result.err.trim();
        if (out.isEmpty()) {
            LOG.warn("stdout: \"\"");
        } else {
            LOG.warn("stdout:");
            LOG.warn(out);
        }
        if (err.isEmpty()) {
            LOG.warn("stderr: \"\"");
        } else {
            LOG.warn("stderr:");
            LOG.warn(err);
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
                        if (!resolvedIp.equals(waitFor.getIp().trim())) {
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

            if (resolvedIp == null || !resolvedIp.trim().equals(waitFor.getIp().trim())) {
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
        try {
            final Execute.ExecutionResult result = Execute.bashQuiet("find_ip", "( host " + domainName + " " + dnsServer + " || echo failed: has address not_found ) | grep \"has address\" | head -n 1 | cut -d \" \" -f 4");
            if (result.out.trim().equals("not_found")) {
                result.code = 101;
            }
            return result;
        } catch (IOException | InterruptedException e) {
            String msg = "Unable to check DNS for " + domainName + " using DNS server " + dnsServer;
            e.printStackTrace();
            System.err.println(msg);
            final Execute.ExecutionResult result = new Execute.ExecutionResult();
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
        // grab the _external_ IP address
        Execute.ExecutionResult result = execute(Arrays.asList(
                "gcloud", "compute", "instances", "describe", "--project=" + getGoogleProject(),
                node.getHost(), "--zone", getGoogleZone(), "--format=value(networkInterfaces[0].accessConfigs[0].natIP)"
        ));
        if (result.code != 0 || result.out.trim().isEmpty()) {
            throw new UnknownHostException(node.getDomainName());
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
    public void replaceDNS(final Machine machine) throws IOException, InterruptedException {
        final DomainMapping domain = machine.getDomainInUse();
        String newName = NameGen.newName();
        dns.tx(tx -> {
            String domainRoot = DOMAIN;
            if (domain != null) {
                // throw this domain away!
                tx.removeRecord(machine.domain(), machine.getIp());
                domainRoot = domain.getDomainRoot();
            }
            DomainMapping newDomain = new DomainMapping(newName, domainRoot);
            machine.setDomainInUse(newDomain);
            tx.addRecord(machine.domain(), machine.getIp());
        });
    }

    public GoogleDnsManager dns() {
        return dns;
    }
}

