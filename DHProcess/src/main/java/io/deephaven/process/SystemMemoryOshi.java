package io.deephaven.process;

import io.deephaven.properties.PropertySet;
import io.deephaven.properties.PropertyVisitor;
import org.immutables.value.Value;
import oshi.hardware.GlobalMemory;

@Value.Immutable
@ProcessStyle
public abstract class SystemMemoryOshi implements PropertySet {

    private static final String PHYSICAL = "physical";
    private static final String SWAP = "swap";
    private static final String PAGE_SIZE = "page-size";

    /**
     * The amount of actual physical memory, in bytes.
     *
     * @return Total number of bytes.
     */
    @Value.Parameter
    public abstract long getPhysicalTotal();

    /**
     * The current size of the paging/swap file(s), in bytes. If the paging/swap file can be extended, this is a soft
     * limit.
     *
     * @return Total swap in bytes.
     */
    @Value.Parameter
    public abstract long getSwapTotal();

    /**
     * The number of bytes in a memory page
     *
     * @return Page size in bytes.
     */
    @Value.Parameter
    public abstract long getPageSize();

    @Override
    public final void traverse(PropertyVisitor visitor) {
        visitor.visit(PHYSICAL, getPhysicalTotal());
        visitor.visit(SWAP, getSwapTotal());
        visitor.visit(PAGE_SIZE, getPageSize());
    }

    public static SystemMemoryOshi from(GlobalMemory memory) {
        return ImmutableSystemMemoryOshi.builder()
                .physicalTotal(memory.getTotal())
                .swapTotal(memory.getVirtualMemory().getSwapTotal())
                .pageSize(memory.getPageSize())
                .build();
    }
}
