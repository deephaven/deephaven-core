package io.deephaven.process;

import io.deephaven.properties.PropertySet;
import io.deephaven.properties.PropertyVisitor;
import org.immutables.value.Value;
import oshi.software.os.OperatingSystem;

@Value.Immutable
@ProcessStyle
public abstract class OperatingSystemOshi implements PropertySet {

    private static final String FAMILY = "family";
    private static final String MANUFACTURER = "manufacturer";
    private static final String VERSION = "version";
    private static final String NETWORK = "network";
    private static final String PID = "pid";

    @Value.Parameter
    public abstract String getFamily();

    @Value.Parameter
    public abstract String getManufacturer();

    @Value.Parameter
    public abstract OperatingSystemVersionOshi getVersion();

    @Value.Parameter
    public abstract NetworkOshi getNetwork();

    @Value.Parameter
    public abstract int getPid();

    @Override
    public final void traverse(PropertyVisitor visitor) {
        visitor.visit(FAMILY, getFamily());
        visitor.visit(MANUFACTURER, getManufacturer());
        visitor.visitProperties(VERSION, getVersion());
        visitor.visitProperties(NETWORK, getNetwork());
        visitor.visit(PID, getPid());
    }

    public static OperatingSystemOshi from(OperatingSystem os) {
        return ImmutableOperatingSystemOshi.builder()
                .family(os.getFamily())
                .manufacturer(os.getManufacturer())
                .version(OperatingSystemVersionOshi.from(os.getVersionInfo()))
                .network(NetworkOshi.from(os.getNetworkParams()))
                .pid(os.getProcessId())
                .build();
    }
}
