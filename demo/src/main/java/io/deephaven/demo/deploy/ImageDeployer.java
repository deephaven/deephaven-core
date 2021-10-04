package io.deephaven.demo.deploy;

import org.apache.commons.io.IOUtils;
import org.jboss.logging.Logger;

import java.io.*;
import java.util.stream.Stream;

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
        concatScripts(localDir, "prepare-worker.sh", "gen-certs.sh", "prepare-worker.sh");
        concatScripts(localDir, "prepare-controller.sh", "gen-certs.sh", "prepare-controller.sh");
        GoogleDeploymentManager manager = new GoogleDeploymentManager(localDir);
        String prefix = machinePrefix + (machinePrefix.isEmpty() || machinePrefix.endsWith("-") ? "" : "-");
        String workerBox = prefix + "worker"; //ancestor-worker
        String controllerBox = prefix + "controller"; // ancestor=controller

        LOG.info("Deleting old boxes " + workerBox +" and " + controllerBox + " if they exist");
        GoogleDeploymentManager.gcloud(true, "instances", "delete", "-q", workerBox);
        GoogleDeploymentManager.gcloud(true, "instances", "delete", "-q", controllerBox);

        LOG.info("Creating new worker template box");
        Machine worker = new Machine(workerBox);
        worker.setSnapshotCreate(true);
        // The manager itself has code to select our prepare-worker.sh script as machine startup script
        manager.createMachine(worker);
        manager.assignDns(Stream.of(worker));
        manager.waitForSsh(worker);
        LOG.info("Worker is ready to be tested and converted to an image. You can test this machine here:" +
                "Web: https://" + worker.getDomainName() + "\n" +
                "ssh " + worker.getDomainName() + " # Only if you've opened port 22, which you should NOT on public internet\n" +
                "gcloud compute ssh " + worker.getHost() + " --project " + GoogleDeploymentManager.getGoogleProject());

        // TODO wait until we can reach /health, do some other kind of testing

//        manager.turnOff(worker);
        // TODO: turn it back on, wait again until /health works, to verify that iptables rules are persisting across restarts


    }

    private void concatScripts(final String into, final String outputFilename, final String ... scripts) throws IOException {
        final File outFile = new File(into, outputFilename);
        outFile.getParentFile().mkdirs();
        outFile.delete();
        try (final FileOutputStream out = new FileOutputStream(outFile)) {
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
                        out.write(bytes, 0, r);
                    }
                    out.write('\n');
                }
            }
        }


    }

}
