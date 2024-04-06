//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.process;

import io.deephaven.process.ImmutableMemoryPoolsMxBeanInfo.Builder;
import io.deephaven.properties.PropertySet;
import io.deephaven.properties.PropertyVisitor;
import java.lang.management.MemoryPoolMXBean;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.immutables.value.Value;
import org.immutables.value.Value.Parameter;

@Value.Immutable
@ProcessStyle
public abstract class MemoryPoolsMxBeanInfo implements PropertySet {

    public static MemoryPoolsMxBeanInfo of(List<MemoryPoolMXBean> pools) {
        final Builder builder = ImmutableMemoryPoolsMxBeanInfo.builder();
        for (MemoryPoolMXBean pool : pools) {
            builder.putUsages(pool.getName(), MemoryUsageInfo.of(pool.getUsage()));
        }
        return builder.build();
    }

    @Parameter
    public abstract Map<String, MemoryUsageInfo> usages();

    @Override
    public void traverse(PropertyVisitor visitor) {
        for (Entry<String, MemoryUsageInfo> e : usages().entrySet()) {
            visitor.visitProperties(e.getKey(), e.getValue());
        }
    }
}
