package io.deephaven.demo.deploy;

import io.deephaven.demo.ClusterController;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * GoogleDeploymentManagerTest:
 * <p>
 * <p>
 */
public class GoogleDeploymentManagerTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Ignore // don't run "create a VM" on regular unit tests... hook this up to be nightly-only
    @Test
    public void testGetWorker() throws IOException, InterruptedException {
        String workDir = tmp.newFolder("deploy").getAbsolutePath();
        GoogleDeploymentManager deploy = new GoogleDeploymentManager(workDir);

        ClusterController ctrl = new ClusterController(deploy);

        long beforeRequest = System.currentTimeMillis();
        Machine machine = ctrl.requestMachine();
        long beforeHealthy = System.currentTimeMillis();
        ctrl.waitUntilHealthy(machine);
        long afterHealthy = System.currentTimeMillis();
        // we survived! The machine is working!

        // TODO: use this machine to do some testing of live system.  Perhaps better moved to a @ClassRule?

        // NOW DESTROY IT! :-)
        ctrl.getDeploymentManager().destroyCluster(machine);
        long afterDestroy = System.currentTimeMillis();

        System.out.println("Waited " + TimeUnit.MILLISECONDS.toSeconds(beforeHealthy - beforeRequest) + "s to get " + machine);
        System.out.println("Waited " + TimeUnit.MILLISECONDS.toSeconds(afterHealthy - beforeHealthy) + "s until seeing a healthy " + machine);
        System.out.println("Waited " + TimeUnit.MILLISECONDS.toSeconds(afterDestroy - afterHealthy) + "s to destroy " + machine);

    }

    @Ignore // Uncomment this to use this method to play with "create a machine" semantics
    @Test
    public void testMachineSetup() throws IOException, InterruptedException, TimeoutException {
        String workDir = tmp.newFolder("deploy").getAbsolutePath();
        GoogleDeploymentManager deploy = new GoogleDeploymentManager(workDir);

        ClusterController ctrl = new ClusterController(deploy, false, true);

        Machine.useImages = false;
        // you can change this machine name to whatever machine name you want
        final Machine testNode = ctrl.requestMachine("test-jxn", true);
//        testNode.setController(true); // feel free to use this to get a controller to test.
        if (!deploy.checkExists(testNode)) {
            deploy.createNew(testNode, deploy.getIpPool());
        }
        deploy.turnOn(testNode);
        deploy.waitForDns(Collections.singletonList(testNode), GoogleDeploymentManager.DNS_GOOGLE);
        final String ipAddr = deploy.getDnsIp(testNode);
        Assert.assertNotNull(ipAddr);
        Assert.assertNotEquals("", ipAddr);

        if ("true".equals(System.getProperty("noClean"))) {
            System.out.println("-DnoClean=true detected, leaving " + testNode.getHost() + " alive;\n" +
                    "Web: https://" + testNode.getDomainName() + "\n" +
                    "ssh " + testNode.getDomainName() + " # Only if you've opened port 22, which you should NOT on public internet\n" +
                    "gcloud compute ssh " + testNode.getHost() + " --project " + GoogleDeploymentManager.getGoogleProject()
            );
        } else {
            System.out.println("You did not set sysprop -DnoClean=true, so deleting " + testNode.getHost());
            deploy.destroyCluster(Collections.singletonList(testNode), "");
        }
    }
}
