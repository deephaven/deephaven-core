package io.deephaven.process;

import io.deephaven.properties.PropertySet;
import io.deephaven.properties.PropertyVisitor;
import org.immutables.value.Value;
import oshi.SystemInfo;

@Value.Immutable
@ProcessStyle
public abstract class SystemInfoOshi implements PropertySet {

    private static final String OS = "os";
    private static final String SYS = "sys";
    private static final String MEMORY = "memory";
    private static final String CPU = "cpu";

    @Value.Parameter
    public abstract OperatingSystemOshi getOperatingSystem();

    @Value.Parameter
    public abstract ComputerSystemOshi getComputerSystem();

    @Value.Parameter
    public abstract SystemMemoryOshi getSystemMemory();

    @Value.Parameter
    public abstract SystemCpuOshi getSystemCpu();

    @Override
    public void traverse(PropertyVisitor visitor) {
        visitor.visitProperties(OS, getOperatingSystem());
        visitor.visitProperties(SYS, getComputerSystem());
        visitor.visitProperties(MEMORY, getSystemMemory());
        visitor.visitProperties(CPU, getSystemCpu());
    }

    public static SystemInfoOshi forCurrentProcess() {
        final SystemInfo info = new SystemInfo();
        return ImmutableSystemInfoOshi.builder()
                .operatingSystem(OperatingSystemOshi.from(info.getOperatingSystem()))
                .computerSystem(ComputerSystemOshi.from(info.getHardware().getComputerSystem()))
                .systemMemory(SystemMemoryOshi.from(info.getHardware().getMemory()))
                .systemCpu(SystemCpuOshi.from(info.getHardware().getProcessor()))
                .build();
    }
}
