package io.deephaven.demo.deploy;

import io.deephaven.demo.ClusterController;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.deephaven.demo.NameConstants.*;

/**
 * ImageDeployer:
 * <p>
 * <p> This class is responsible for building new worker/controller images.
 * <p>
 * <p> It starts with a blank machine, applies our setup scripts, then turn the machine
 * <p> off and creates a new image of the harddrive, so we can easily spin up N pre-setup copies.
 *
 */
public class ImageDeployer {

    static {
        System.setProperty("org.jboss.logging.provider", "jdk");
    }
    private static final Logger LOG = Logger.getLogger(ImageDeployer.class);

    public static void main(String ... args) throws IOException, InterruptedException {
        final ImageDeployer deployer = new ImageDeployer();
        if (args.length == 0) {
            deployer.deploy(VERSION, "ancestor");
        } else {
            if ("-w".equals(args[0]) || "--worker".equals(args[0])) {
                if (args.length < 2) {
                    throw new IllegalArgumentException("Sent " + args[0] + " without a second argument");
                }
                String workerName = args[1];
                deployer.deploySingleWorker(workerName);
            } else {
                deployer.deploy(args[0], args.length > 1 ? args[1] : "ancestor");
            }
        }
    }

    private void deploySingleWorker(final String workerName) throws IOException, InterruptedException {
        LOG.infof("Creating new worker %s", workerName);
        final String localDir = System.getProperty("java.io.tmpdir", "/tmp") + "/dh_deploy_" + workerName;
        GoogleDeploymentManager manager = new GoogleDeploymentManager(localDir);
        if ("true".equals(System.getProperty("rebuildWorker"))) {
            manager.deleteMachine(workerName);
        }
        ClusterController ctrl = new ClusterController(manager, false, true);
        final Machine machine = ctrl.requestMachine(workerName, true, false);
        manager.waitForSsh(machine);
        ctrl.waitUntilHealthy(machine);
        LOG.infof("\n\nYour machine %s is healthy!", machine.getDomainName());
        LOG.infof("Visit it on the web: https://%s\n\n", machine.getDomainName());
    }

    private void deploy(final String version, String machinePrefix) throws IOException, InterruptedException {
        final String localDir = System.getProperty("java.io.tmpdir", "/tmp") + "/dh_deploy_" + version;
        GoogleDeploymentManager manager = new GoogleDeploymentManager(localDir);
        ClusterController ctrl = new ClusterController(manager, false, true);
        String prefix = machinePrefix + (machinePrefix.isEmpty() || machinePrefix.endsWith("-") ? "" : "-");
        String workerBox = prefix + "worker-" + VERSION_MANGLE; //ancestor-worker
        String controllerBox = prefix + "controller-" + VERSION_MANGLE; // ancestor=controller
        String baseBox = manager.getBaseImageName();
        manager.setBaseImageName(baseBox);
        final boolean workerOnly = "true".equals(System.getProperty("workerOnly"));
        final boolean force = "true".equals(System.getProperty("force"));

        Execute.ExecutionResult result;
        if (!force && !workerOnly) {
            // for now, we are NOT going to allow stomping worker images w/o explicit -Pforce=true
            result = GoogleDeploymentManager.gcloudQuiet(true, false, "images", "describe", SNAPSHOT_NAME + "-worker");
            if (result.code == 0) {
                throw new IllegalStateException("Snapshot " + SNAPSHOT_NAME + "-worker already exists; please bump your version!");
            }
        }

        if (!workerOnly) {
            // workers _can_ create themselves w/o a base image.
            result = GoogleDeploymentManager.gcloudQuiet(true, false,
                    "images", "describe", baseBox);
            if (result.code != 0) {
                // create a base box for the given version.
                LOG.infof("No base image for %s, creating new base image");
                manager.deleteMachine(baseBox);
                Machine base = ctrl.findMachine(baseBox, true, true);
                base.getIp().setDomain(new DomainMapping(baseBox, DOMAIN));
                base.setSnapshotCreate(true);
                manager.createMachine(base, manager.getIpPool());
                manager.assignDns(ctrl, Stream.of(base));
                // even if we're just going to shut the machine down, wait until ssh is responsive
                manager.waitForSsh(base, -1, TimeUnit.MINUTES.toMillis(15));
                // wait until we can get a finish-setup.sh "we are done" log messages.
                ctrl.waitUntilHealthy(base);
                // must delete the vm-startup.log link, otherwise a later worker or controller image might accidentally think it is already done setup!
                // basically, .waitUntilHealthy polls this log for a finished message, so we do NOT want that in the base image (it will cause race conditions).
                Execute.ssh(false, base.getDomainName(), "sudo rm /var/log/vm-startup.log");
                // deploy the base image.
                finishDeploy("Base", base, manager);
            }
        }

        Machine controller;
        if (workerOnly) {
            controller = null;
        } else {
            controller = ctrl.findMachine(controllerBox, true, true);
            LOG.info("Deleting old boxes " + workerBox +" and " + controllerBox + " if they exist");
            // lots of time until we create the controller box, off-thread this one so we can get to the good stuff
            ClusterController.setTimer("Delete " + controllerBox, ()-> {
                GoogleDeploymentManager.gcloud(true, "instances", "delete", "-q",
    //                    "controller-" + VERSION_MANGLE
                        controllerBox
                );
                controller.getIp().setDomain(new DomainMapping(controllerBox, DOMAIN));
                // The manager itself has code to select our prepare-controller.sh script as machine startup script based on these bools:
                controller.setController(true);
                controller.setSnapshotCreate(true);

                return "";
            });
        }
//        // no need to offthread, the next "expensive" operation we do is to create a clean box.
//        // if we later create a -base image for both, we would offthread the worker, and do the baseBox in this thread.
        GoogleDeploymentManager.gcloud(true, "instances", "delete", "-q", workerBox);


        LOG.info("Creating new worker template box");
        Machine worker = ctrl.findMachine(workerBox, true, true);

        worker.getIp().setDomain(new DomainMapping(workerBox, DOMAIN));
        worker.setSnapshotCreate(true);
        // The manager itself has code to select our prepare-worker.sh script as machine startup script
        manager.createMachine(worker, manager.getIpPool());
        manager.assignDns(ctrl, Stream.of(worker));
        // even if we're just going to shut the machine down, wait until ssh is responsive
        manager.waitForSsh(worker, -1, TimeUnit.MINUTES.toMillis(15));
        // wait until we can reach /health, so we know the system setup is complete and the server is in a running state.
        ctrl.waitUntilHealthy(worker);
        // TODO: have a test to turn machine off and on, wait again until /health works, to verify that iptables rules are persisting across restarts

        finishDeploy("Worker", worker, manager);

        if (workerOnly) {
            LOG.info("Done deploying new worker image only, exiting early.");
            return;
        }
        // worker is done, create the controller (we already setup DNS for it above)
        LOG.info("Creating new controller template box");
        manager.createMachine(controller, manager.getIpPool());
        manager.assignDns(ctrl, Stream.of(controller));

        manager.waitForSsh(controller, -1, TimeUnit.MINUTES.toMillis(15));
        ctrl.waitUntilHealthy(controller);

        finishDeploy("Controller", controller, manager);

        LOG.info("Creating new controller-" + VERSION_MANGLE + " box");
        final String controllerHost = "controller-" + VERSION_MANGLE;
        if (force) {
            manager.deleteMachine(controllerHost);
        }
        Machine newCtrl = ctrl.findMachine(controllerHost, true, true);
        if (newCtrl.getIp() == null) {
            newCtrl.setIp(ctrl.requestIp());
        }
        LOG.info("Setting up domain for " + newCtrl);
        // this is not backed by a naturally-built DNS record. Need to test...
        newCtrl.getIp().setDomain(new DomainMapping(newCtrl.getHost(), DOMAIN));
        newCtrl.setController(true);
        LOG.info("Creating " + newCtrl);
        manager.createMachine(newCtrl, manager.getIpPool());
        LOG.infof("Assign DNS to %s with domain %s", newCtrl.getHost(), newCtrl.domain());
        manager.assignDns(ctrl, Stream.of(newCtrl));

        manager.waitForSsh(newCtrl, TimeUnit.MINUTES.toMillis(3), TimeUnit.MINUTES.toMillis(7));

        LOG.infof("Destroying VMs %s and %s", worker, controller);
        manager.destroyCluster(Arrays.asList(worker, controller), "");

        LOG.infof("Done deployment!\n\nTest https://%s.%s and promote to leader by updating DNS for %s to point to %s\n\n",
                newCtrl.getHost(), newCtrl.domain().getDomainRoot(), newCtrl.domain().getDomainRoot(), newCtrl.getIp().getIp());
    }

    private void finishDeploy(final String type, final Machine machine, final DeploymentManager manager) throws IOException, InterruptedException {
        boolean doDeploy = "true".equals(System.getProperty("deployImage")) || "true".equals(System.getenv("DEPLOY_IMAGE"));
        LOG.info("Finishing deploy for " + machine);
        final String typeLower = type.toLowerCase();
        final String typeUpper = type.toUpperCase();
        String snapName = "base".equals(typeLower) ? machine.getHost() : SNAPSHOT_NAME + "-" + typeLower;
        if (doDeploy) {
            LOG.infof("Creating new %s image %s", typeLower, snapName);
            manager.turnOff(machine);
            GoogleDeploymentManager.gcloud(true, false,"images", "delete", "-q", snapName);
            GoogleDeploymentManager.gcloud(false, false,"images", "create", snapName,
                    "--source-disk", machine.getHost(), "--source-disk-zone", GoogleDeploymentManager.getGoogleZone());
            LOG.infof("Done creating new %s image %s", typeLower, snapName);

        } else {
            LOG.warnf("NOT DEPLOYING %s IMAGE, to deploy, set -DdeployImage=true or env DEPLOY_IMAGE=true", typeUpper);
            if (LOG.isInfoEnabled()) {
                LOG.info(type + " is ready to be tested and converted to an image. You can test this machine here:\n" +
                        "Web: https://" + machine.getDomainName() + "\n" +
                        "ssh " + machine.getDomainName() + " # Only if you've opened port 22, which you should NOT do on public internet\n" +
                        "gcloud compute ssh " + machine.getHost() + " --project " + GoogleDeploymentManager.getGoogleProject());
            }
        }
    }

}
