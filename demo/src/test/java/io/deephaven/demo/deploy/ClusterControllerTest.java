package io.deephaven.demo.deploy;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.deephaven.demo.ClusterController.parseTime;
import static io.deephaven.demo.ClusterController.toTime;

/**
 * ClusterControllerTest:
 * <p><p>
 */
public class ClusterControllerTest {
    @Test
    public void testDateParsing() {
        long originalTime = System.currentTimeMillis();
        final String timeString = toTime(originalTime);
        System.out.println(timeString);
        final long parsedTime = parseTime(timeString, getClass().getSimpleName());
        System.out.println(parsedTime);
        System.out.println(originalTime);
        // we lose millisecond precision w/ concise yyMMdd-HHmmss NY-time-zoned time strings (we only use time parsing for leases-by-the-minute)
        Assert.assertEquals((originalTime/1000)*1000, parsedTime);
    }

    @Test
    public void testMachineRecycling() {
        MachinePool machines = new MachinePool();
        IpPool ips = new IpPool();
        Machine a = new Machine("a", ips.updateOrCreate("a", "1.2.3.4"));
        Machine b = new Machine("b", ips.updateOrCreate("b", "1.2.3.5"));
        a.setInUse(true);
        b.setInUse(true);
        machines.addMachine(a);
        machines.addMachine(b);
        List<Machine> machineList = machines.getAllMachines().collect(Collectors.toList());
        Assert.assertEquals(machineList, Arrays.asList(a, b));

        // now, expire b w/ a value older than b
        machines.expireInMillis(a, -1000);
        machines.expireInMillis(b, 1000);
        machineList = machines.getAllMachines().collect(Collectors.toList());
        Assert.assertEquals(machineList, Arrays.asList(b, a));


    }
}
