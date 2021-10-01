package io.deephaven.demo.deploy;

import io.deephaven.demo.ClusterController;
import io.deephaven.demo.NameGen;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * GoogleDeploymentManagerTest:
 * <p>
 * <p>
 * Created by James X. Nelson (James@WeTheInter.net) on 21/09/2021 @ 3:51 p.m..
 */
public class GoogleDeploymentManagerTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void testMachineSetup() throws IOException, InterruptedException, TimeoutException {
        final ClusterMap map = new ClusterMap();
        map.setClusterName(NameGen.newName());
        map.setLocalDir(tmp.newFolder("deploy").getAbsolutePath());
        GoogleDeploymentManager deploy = new GoogleDeploymentManager(map.getLocalDir());

        ClusterController ctrl = new ClusterController(deploy);

        Machine machine = ctrl.requestMachine();


        final Machine testNode = new Machine();
        testNode.setHost("controller3");
        testNode.setController(true);
        testNode.setDomainName("ctrl.demo.deephaven.app");
        map.setAllNodes(Collections.singletonList(testNode));
        if (!deploy.checkExists(testNode)) {
            deploy.createNew(testNode);
        }
        deploy.turnOn(testNode);
        deploy.assignDns(map);
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
            deploy.destroyCluster(map, "");
        }
    }
}
