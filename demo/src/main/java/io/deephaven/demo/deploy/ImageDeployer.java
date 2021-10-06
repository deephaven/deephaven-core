package io.deephaven.demo.deploy;

import io.deephaven.demo.ClusterController;
import org.jboss.logging.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.deephaven.demo.NameConstants.SNAPSHOT_NAME;
import static io.deephaven.demo.NameConstants.VERSION_MANGLE;

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

    private static final Logger LOG = Logger.getLogger(ImageDeployer.class);

    public static void main(String ... args) throws IOException, InterruptedException {
        new ImageDeployer().deploy(args.length > 0 ? args[0] : "0.0.1",
                                  args.length > 1 ? args[1] : "ancestor");
    }

    private void deploy(final String version, String machinePrefix) throws IOException, InterruptedException {
        final String localDir = System.getProperty("java.io.tmpdir", "/tmp") + "/dh_deploy_" + version;
        concatScripts(localDir, "prepare-worker.sh",
                "script-header.sh",
                "setup-docker.sh",
                "get-credentials.sh",
                "gen-certs.sh",
                "prepare-worker.sh",
                "pull-images.sh",
                "finish-setup.sh");
        concatScripts(localDir, "prepare-controller.sh",
                "script-header.sh",
                "setup-docker.sh",
                "get-credentials.sh",
                "gen-certs.sh",
                "prepare-controller.sh",
                "pull-images.sh",
                "finish-setup.sh");
        GoogleDeploymentManager manager = new GoogleDeploymentManager(localDir);
        ClusterController ctrl = new ClusterController(manager, false);
        String prefix = machinePrefix + (machinePrefix.isEmpty() || machinePrefix.endsWith("-") ? "" : "-");
        String workerBox = prefix + "worker"; //ancestor-worker
        String controllerBox = prefix + "controller"; // ancestor=controller

        LOG.info("Deleting old boxes " + workerBox +" and " + controllerBox + " if they exist");
        // lots of time until we create the controller box, off-thread this one so we can get to the good stuff
//        ClusterController.setTimer("Delete " + controllerBox, ()->
//            GoogleDeploymentManager.gcloud(true, "instances", "delete", "-q", controllerBox)
//        );
//        // no need to offthread, the next "expensive" operation we do is to create a clean box.
//        // if we later create a -base image for both, we would offthread the worker, and do the baseBox in this thread.
//        GoogleDeploymentManager.gcloud(true, "instances", "delete", "-q", workerBox);
//
//
//        LOG.info("Creating new worker template box");
//        final IpMapping workerIp = ctrl.requestIp();
//        Machine worker = new Machine(workerBox);
//        worker.setIp(workerIp.getName());
//        worker.setSnapshotCreate(true);
//        // The manager itself has code to select our prepare-worker.sh script as machine startup script
//        manager.createMachine(worker);
//        manager.assignDns(Stream.of(worker));
//        // even if we're just going to shut the machine down, wait until ssh is responsive
//        manager.waitForSsh(worker, TimeUnit.MINUTES.toMillis(10), TimeUnit.MINUTES.toMillis(15));
//        // wait until we can reach /health, so we know the system setup is complete and the server is in a running state.
//        ctrl.waitUntilHealthy(worker);
//        // TODO: have a test to turn machine off and on, wait again until /health works, to verify that iptables rules are persisting across restarts
//
//        finishDeploy("Worker", worker, manager);
//
//        // worker is done, do the controller
//        LOG.info("Creating new controller template box");
//        final IpMapping controllerIp = ctrl.requestIp();
//        Machine controller = new Machine(controllerBox);
//        controller.setIp(controllerIp.getName());
//        // The manager itself has code to select our prepare-controller.sh script as machine startup script based on these bools:
//        controller.setController(true);
//        controller.setSnapshotCreate(true);
//        manager.createMachine(controller);
//        manager.assignDns(Stream.of(controller));
//        manager.waitForSsh(controller, TimeUnit.MINUTES.toMillis(10), TimeUnit.MINUTES.toMillis(15));
//        ctrl.waitUntilHealthy(controller);
//
//        finishDeploy("Controller", controller, manager);

        Machine newCtrl = new Machine("controller-" + VERSION_MANGLE);
        newCtrl.setIp(ctrl.requestIp().getName());
        newCtrl.setController(true);
        manager.createMachine(newCtrl);
        manager.assignDns(Stream.of(newCtrl));

    }

    private void finishDeploy(final String type, final Machine machine, final DeploymentManager manager) throws IOException, InterruptedException {
        boolean doDeploy = "true".equals(System.getProperty("deployImage")) || "true".equals(System.getenv("DEPLOY_IMAGE"));
        LOG.info("Finishing deploy for " + machine);
        final String typeLower = type.toLowerCase();
        final String typeUpper = type.toUpperCase();
        String snapName = SNAPSHOT_NAME + "-" + typeLower;
        if (doDeploy) {
            LOG.infof("Creating new %s image %s", typeLower, snapName);
            manager.turnOff(machine);
            GoogleDeploymentManager.gcloud(true, false,"images", "delete", "-q", snapName);
            GoogleDeploymentManager.gcloud(false, false,"images", "create", snapName,
                    "--source-disk", machine.getHost(), "--source-disk-zone", GoogleDeploymentManager.getGoogleZone());
            LOG.infof("Done creating new %s image %s", typeLower, snapName);

            LOG.infof("Destroying VM %s", machine);
            manager.destroyCluster(Collections.singletonList(machine), "");
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

        try (final FileOutputStream out = new FileOutputStream(outFile, true)) {
            for (String script : scripts) {
                try(final InputStream in = ImageDeployer.class.getResourceAsStream("/scripts/" + script)) {
                    if (in == null) {
                        throw new NullPointerException("No scripts/" + script + " file");
                    }
                    int num = Math.max(4096, in.available());
                    final byte[] bytes = new byte[num];
                    for (int r;
                         (r = in.read(bytes, 0, num)) > 0;
                    ) {
                        LOG.infof("Wrote %s bytes from scripts/%s to %s", r, script, outputFilename);
                        out.write(bytes, 0, r);
                    }
                    out.write('\n');
                }
            }
        }


    }

}
