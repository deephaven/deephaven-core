package io.deephaven.process;

import io.deephaven.properties.PropertySet;
import io.deephaven.properties.PropertyVisitor;
import java.lang.management.MemoryMXBean;
import org.immutables.value.Value;

@Value.Immutable
@ProcessStyle
public abstract class MemoryMxBeanInfo implements PropertySet {

    private static final String HEAP = "heap";
    private static final String NONHEAP = "non-heap";

    public static MemoryMxBeanInfo of(MemoryMXBean bean) {
        return ImmutableMemoryMxBeanInfo.builder()
            .heap(MemoryUsageInfo.of(bean.getHeapMemoryUsage()))
            .nonHeap(MemoryUsageInfo.of(bean.getNonHeapMemoryUsage()))
            .build();
    }

    @Value.Parameter
    public abstract MemoryUsageInfo heap();

    @Value.Parameter
    public abstract MemoryUsageInfo nonHeap();

    @Override
    public final void traverse(PropertyVisitor visitor) {
        visitor.visitProperties(HEAP, heap());
        visitor.visitProperties(NONHEAP, nonHeap());
    }
}
