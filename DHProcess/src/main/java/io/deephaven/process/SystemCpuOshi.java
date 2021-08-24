package io.deephaven.process;

import io.deephaven.properties.PropertySet;
import io.deephaven.properties.PropertyVisitor;
import java.util.OptionalLong;
import org.immutables.value.Value;
import oshi.hardware.CentralProcessor;
import oshi.hardware.CentralProcessor.ProcessorIdentifier;

/**
 * The Central Processing Unit (CPU) or the processor is the portion of a computer system that
 * carries out the instructions of a computer program, and is the primary element carrying out the
 * computer's functions.
 */
@Value.Immutable
@ProcessStyle
public abstract class SystemCpuOshi implements PropertySet {

    private static final String VENDOR = "vendor";
    private static final String NAME = "name";
    private static final String PROCESSOR_ID = "processor-id";
    private static final String STEPPING = "stepping";
    private static final String MODEL = "model";
    private static final String FAMILY = "family";
    private static final String LOGICAL = "logical";
    private static final String PHYSICAL = "physical";
    private static final String SOCKETS = "sockets";
    private static final String IS_64_BIT = "is64bit";
    private static final String VENDOR_FREQUENCY = "vendor-frequency";

    /**
     * Processor vendor.
     *
     * @return vendor string.
     */
    @Value.Parameter
    public abstract String getVendor();

    /**
     * Name, eg. Intel(R) Core(TM)2 Duo CPU T7300 @ 2.00GHz
     *
     * @return Processor name.
     */
    @Value.Parameter
    public abstract String getName();

    /**
     * Vendor frequency (in Hz), eg. for processor named Intel(R) Core(TM)2 Duo CPU T7300 @ 2.00GHz
     * the vendor frequency is 2000000000.
     *
     * @return Processor frequency, if known
     */
    @Value.Parameter
    public abstract OptionalLong getVendorFreq();

    /**
     * Gets the Processor ID. This is a hexidecimal string representing an 8-byte value, normally
     * obtained using the CPUID opcode with the EAX register set to 1. The first four bytes are the
     * resulting contents of the EAX register, which is the Processor signature, represented in
     * human-readable form by {@link #getIdentifier()} . The remaining four bytes are the contents
     * of the EDX register, containing feature flags.
     *
     * NOTE: The order of returned bytes is platform and software dependent. Values may be in either
     * Big Endian or Little Endian order.
     *
     * @return A string representing the Processor ID
     */
    @Value.Parameter
    public abstract String getProcessorID();

    /**
     * @return the stepping
     */
    @Value.Parameter
    public abstract String getStepping();

    /**
     * @return the model
     */
    @Value.Parameter
    public abstract String getModel();

    /**
     * @return the family
     */
    @Value.Parameter
    public abstract String getFamily();

    /**
     * Get the number of logical CPUs available for processing. This value may be higher than
     * physical CPUs if hyperthreading is enabled.
     *
     * @return The number of logical CPUs available.
     */
    @Value.Parameter
    public abstract int getLogicalProcessorCount();

    /**
     * Get the number of physical CPUs/cores available for processing.
     *
     * @return The number of physical CPUs available.
     */
    @Value.Parameter
    public abstract int getPhysicalProcessorCount();

    /**
     * Get the number of packages/sockets in the system. A single package may contain multiple
     * cores.
     *
     * @return The number of physical packages available.
     */
    @Value.Parameter
    public abstract int getPhysicalPackageCount();

    /**
     * Is CPU 64bit?
     *
     * @return True if cpu is 64bit.
     */
    @Value.Parameter
    public abstract boolean is64bit();

    @Override
    public final void traverse(PropertyVisitor visitor) {
        visitor.visit(VENDOR, getVendor());
        visitor.visit(NAME, getName());
        visitor.visit(PROCESSOR_ID, getProcessorID());
        visitor.visit(STEPPING, getStepping());
        visitor.visit(MODEL, getModel());
        visitor.visit(FAMILY, getFamily());
        visitor.visit(LOGICAL, getLogicalProcessorCount());
        visitor.visit(PHYSICAL, getPhysicalProcessorCount());
        visitor.visit(SOCKETS, getPhysicalPackageCount());
        visitor.visit(IS_64_BIT, is64bit());
        visitor.maybeVisit(VENDOR_FREQUENCY, getVendorFreq());
    }

    public static SystemCpuOshi from(CentralProcessor centralProcessor) {
        final ProcessorIdentifier identifier = centralProcessor.getProcessorIdentifier();
        return ImmutableSystemCpuOshi.builder()
            .vendor(identifier.getVendor())
            .name(identifier.getName())
            .processorID(identifier.getProcessorID())
            .stepping(identifier.getStepping())
            .model(identifier.getModel())
            .family(identifier.getFamily())
            .logicalProcessorCount(centralProcessor.getLogicalProcessorCount())
            .physicalProcessorCount(centralProcessor.getPhysicalProcessorCount())
            .physicalPackageCount(centralProcessor.getPhysicalPackageCount())
            .is64bit(identifier.isCpu64bit())
            .vendorFreq(identifier.getVendorFreq() == -1 ? OptionalLong.empty()
                : OptionalLong.of(identifier.getVendorFreq()))
            .build();
    }
}
