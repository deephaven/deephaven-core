package io.deephaven.demo.deploy;

import io.deephaven.demo.ClusterController;
import io.deephaven.demo.NameConstants;
import org.apache.commons.io.IOUtils;
import org.jboss.logging.Logger;

import java.io.*;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.deephaven.demo.NameConstants.SNAPSHOT_NAME;

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
        GoogleDeploymentManager.gcloud(true, "instances", "delete", "-q", workerBox);
        GoogleDeploymentManager.gcloud(true, "instances", "delete", "-q", controllerBox);

        LOG.info("Creating new worker template box");
        final IpMapping ip = ctrl.requestIp();
        Machine worker = new Machine(workerBox);
        worker.setIp(ip.getName());
        worker.setSnapshotCreate(true);
        // The manager itself has code to select our prepare-worker.sh script as machine startup script
        manager.createMachine(worker);
        manager.assignDns(Stream.of(worker));
        manager.waitForSsh(worker, TimeUnit.MINUTES.toMillis(10), TimeUnit.MINUTES.toMillis(15));

        LOG.info("Worker is ready to be tested and converted to an image. You can test this machine here:\n" +
                "Web: https://" + worker.getDomainName() + "\n" +
                "ssh " + worker.getDomainName() + " # Only if you've opened port 22, which you should NOT on public internet\n" +
                "gcloud compute ssh " + worker.getHost() + " --project " + GoogleDeploymentManager.getGoogleProject());

        // TODO wait until we can reach /health, do some other kind of testing
        // TODO: turn it back on, wait again until /health works, to verify that iptables rules are persisting across restarts


        manager.turnOff(worker);
        GoogleDeploymentManager.gcloud(true, false,"images", "delete", "-q", SNAPSHOT_NAME);
        GoogleDeploymentManager.gcloud(false, false,"images", "create", SNAPSHOT_NAME,
                "--source-disk", worker.getHost(), "--source-disk-zone", GoogleDeploymentManager.getGoogleZone());

        manager.destroyCluster(Collections.singletonList(worker), "");
        LOG.info("Done creating new worker image " + SNAPSHOT_NAME);


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
