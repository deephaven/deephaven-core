package io.deephaven.demo.manager;

import io.deephaven.demo.api.IpMapping;
import org.jboss.logging.Logger;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

import static io.deephaven.demo.manager.ControllerTimeTools.*;
import static io.deephaven.demo.manager.NameConstants.*;

/**
 * IClusterController:
 * <p>
 * <p> A minimal interface for what a ClusterController can do,
 * <p> so we can pass around references to an object defined in a downstream module.
 */
public interface IClusterController {
    DeploymentManager getDeploymentManager();

    IpMapping requestIp();

    static long parseTime(String lease, final String debugString) {
        final LocalDateTime time;
        if (!lease.startsWith("20")) {
            // temporary fix so we don't have to throw away all our machines
            lease = "20" + lease;
        }
        try {
            if (!lease.startsWith("202")) {
                LOG.errorf("INVALID LEASE FORMAT: \"%s\" for %s must start with 202 unless it's the 2030s by now", lease, debugString);
                // purposely set this expiry way in the past.
                return System.currentTimeMillis() - 500_000_000L;
            }
            time = LocalDateTime.from(fmt.parse(lease));
        } catch (Throwable t) {
            throw new IllegalArgumentException("Time string \"" + lease + "\" invalid, should match: " + TIME_FMT, t);
        }
        final ZonedDateTime sameLocal = time.atZone(ControllerTimeTools.TZ_NY);
        final Instant localInstant = sameLocal.toInstant();
        //noinspection UnnecessaryLocalVariable
        final long epochMilli = localInstant.toEpochMilli();
        return epochMilli;
    }
    static String toTime(long timestamp) {
        return fmt.format(Instant.ofEpochMilli(timestamp).atZone(TZ_NY));
    }

    void waitUntilIpsCreated();

    default boolean isPeakHours() {
        LocalTime time = LocalTime.now(TZ_NY);
        return time.isAfter(BIZ_START) && time.isBefore(BIZ_END);
    }

    default boolean allowedToDelete(final String version, String machine, Long expiry) {
        if (expiry != null && System.currentTimeMillis() < expiry) {
            LOG.infof("Not shutting down machine with expiry (%s); Full machine: %s",
                    ZonedDateTime.ofInstant(Instant.ofEpochMilli(expiry), TZ_NY), machine);
            return false;
        }
        if (VERSION_MANGLE.contains("_")) {
            if (version.equals(VERSION_MANGLE)) {
                return true;
            }
            LOG.infof("Not shutting down machine with different custom version (%s) than us (%s). Full machine: %s", version, VERSION_MANGLE, machine);
            return false;
        }
        if (version.contains("_")) {
            LOG.infof("Not shutting down machine with custom version (%s). Full machine: %s", version, machine);
            return false;
        }
        String[] ourV = VERSION_MANGLE.split("-"), yourV = version.split("-");
        for (int i = 0; i < ourV.length; i++) {
            if (i >= yourV.length) {
                LOG.errorf("Not shutting down machine with invalid version format (%s). Full machine: %s", version, machine);
                return false;
            }
            final int ourN = Integer.parseInt(ourV[i]);
            final int yourN = Integer.parseInt(yourV[i]);
            if (ourN < yourN) {
                LOG.infof("Not shutting down machine with newer version (%s) than us (%s). Full machine: %s", version, VERSION_MANGLE, machine);
                return false;
            }
            if (ourN > yourN) {
                // if major/minor version is higher, we don't want to check minor/patch
                return true;
            }
        }
        return true;
    }
}
// define a package-local class to hold constants we don't want to be part of our public abi.
class ControllerTimeTools {
    static final Logger LOG = Logger.getLogger(IClusterController.class);

    static final String TIME_FMT = "yyyyMMdd-HHmmss";
    static final DateTimeFormatter fmt =
            new DateTimeFormatterBuilder()
                    .appendPattern("yyyyMMdd")
                    .appendLiteral("-")
                    .appendPattern("HHmmss")
                    .toFormatter();
    static ZoneId TZ_NY = ZoneId.of("America/New_York");
    static LocalTime BIZ_START = LocalTime.of(6, 0);
    // We are using NY timezone, but want to cover all N.A. business hours, so our end is 9pm NY
    static LocalTime BIZ_END = LocalTime.of(21, 0);

}