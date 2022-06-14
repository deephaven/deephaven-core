/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.process;

import io.deephaven.process.ImmutableMemoryUsageInfo.Builder;
import io.deephaven.properties.PropertySet;
import io.deephaven.properties.PropertyVisitor;
import java.util.OptionalLong;
import org.immutables.value.Value;

@Value.Immutable
@ProcessStyle
public abstract class MemoryUsageInfo implements PropertySet {

    private static final String INIT = "init";
    private static final String MAX = "max";

    public static MemoryUsageInfo of(java.lang.management.MemoryUsage usage) {
        final Builder builder = ImmutableMemoryUsageInfo.builder();
        if (usage.getInit() != -1) {
            builder.init(usage.getInit());
        }
        if (usage.getMax() != -1) {
            builder.max(usage.getMax());
        }
        return builder.build();
    }

    @Value.Parameter
    public abstract OptionalLong init();

    @Value.Parameter
    public abstract OptionalLong max();

    @Override
    public final void traverse(PropertyVisitor visitor) {
        visitor.maybeVisit(INIT, init());
        visitor.maybeVisit(MAX, max());
    }
}
