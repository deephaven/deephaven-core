package io.deephaven.demo.deploy;

import org.junit.Assert;
import org.junit.Test;

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
}
