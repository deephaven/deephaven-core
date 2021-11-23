package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.util.compare.ObjectComparisons;
import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import io.deephaven.engine.table.impl.locations.UnknownPartitionKeyException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

/**
 * Base {@link ImmutableTableLocationKey} implementation for table locations that may be enclosed by partitions.
 * Sub-classes should be sure to invoke the partition-map comparator at higher priority than other comparisons when
 * implementing {@link #compareTo(Object)}, and to include the partitions in their {@link #equals(Object)}
 * implementations.
 */
public abstract class PartitionedTableLocationKey implements ImmutableTableLocationKey {

    @SuppressWarnings("StringOperationCanBeSimplified")
    private static final Comparable<?> MISSING_PARTITION_VALUE = new String("MISSING PARTITION VALUE");

    protected final Map<String, Comparable<?>> partitions;

    /**
     * Construct a new PartitionedTableLocationKey for the supplied {@code partitions}.
     *
     * @param partitions The table partitions enclosing the table location keyed by {@code this}. Note that if this
     *        parameter is {@code null}, the location will be a member of no partitions. An ordered copy of the map will
     *        be made, so the calling code is free to mutate the map after this call completes, but the partition keys
     *        and values themselves <em>must</em> be effectively immutable.
     */
    protected PartitionedTableLocationKey(@Nullable final Map<String, Comparable<?>> partitions) {
        this.partitions = partitions == null || partitions.isEmpty() ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(partitions));
    }

    @Override
    public final <PARTITION_VALUE_TYPE extends Comparable<PARTITION_VALUE_TYPE>> PARTITION_VALUE_TYPE getPartitionValue(
            @NotNull final String partitionKey) {
        final Object partitionValue = partitions.getOrDefault(partitionKey, MISSING_PARTITION_VALUE);
        if (partitionValue == MISSING_PARTITION_VALUE) {
            throw new UnknownPartitionKeyException(partitionKey, this);
        }
        // noinspection unchecked
        return (PARTITION_VALUE_TYPE) partitionValue;
    }

    @Override
    public final Set<String> getPartitionKeys() {
        return partitions.keySet();
    }

    /**
     * Formats a map of partitions as key-value pairs.
     */
    protected static final class PartitionsFormatter implements LogOutput.ObjFormatter<Map<String, Comparable<?>>> {

        public static final LogOutput.ObjFormatter<Map<String, Comparable<?>>> INSTANCE = new PartitionsFormatter();

        private PartitionsFormatter() {}

        @Override
        public void format(@NotNull final LogOutput logOutput, @NotNull final Map<String, Comparable<?>> partitions) {
            if (partitions.isEmpty()) {
                logOutput.append("{}");
                return;
            }

            logOutput.append('{');

            for (final Iterator<Map.Entry<String, Comparable<?>>> pi = partitions.entrySet().iterator(); pi
                    .hasNext();) {
                final Map.Entry<String, Comparable<?>> partition = pi.next();
                final String partitionKey = partition.getKey();
                final Comparable<?> partitionValue = partition.getValue();
                logOutput.append(partitionKey).append('=');

                if (partitionValue == null) {
                    logOutput.append("null");
                } else if (partitionValue instanceof CharSequence) {
                    logOutput.append((CharSequence) partitionValue);
                } else if (partitionValue instanceof Long || partitionValue instanceof Integer
                        || partitionValue instanceof Short || partitionValue instanceof Byte) {
                    logOutput.append(((Number) partitionValue).longValue());
                } else if (partitionValue instanceof Double || partitionValue instanceof Float) {
                    logOutput.appendDouble(((Number) partitionValue).doubleValue());
                } else if (partitionValue instanceof Character) {
                    logOutput.append(((Character) partitionValue).charValue());
                } else if (partitionValue instanceof LogOutputAppendable) {
                    logOutput.append((LogOutputAppendable) partitionValue);
                } else {
                    // Boolean, DateTime, or some other arbitrary object we haven't considered
                    logOutput.append(partitionValue.toString());
                }

                logOutput.append(pi.hasNext() ? ',' : '}');
            }
        }
    }

    /**
     * Compares two maps of partitions by key-value pairs. {@code p1}'s entry order determines the priority of each
     * partition in the comparison, and it's assumed that {@code p2} will have the same entry order.
     * {@link #compare(Map, Map)} will throw an {@link UnknownPartitionKeyException} if one of the maps is missing keys
     * found in the other.
     */
    protected static final class PartitionsComparator implements Comparator<Map<String, Comparable<?>>> {

        public static final Comparator<Map<String, Comparable<?>>> INSTANCE = new PartitionsComparator();

        private PartitionsComparator() {}

        private static void checkSizeMismatch(@NotNull final Map<String, Comparable<?>> p1,
                final int p1Size,
                @NotNull final Map<String, Comparable<?>> p2,
                final int p2Size) {
            if (p1Size > p2Size) {
                // noinspection OptionalGetWithoutIsPresent
                throw new UnknownPartitionKeyException(
                        p1.keySet().stream().filter(pk -> !p2.containsKey(pk)).findFirst().get());
            }
        }

        @Override
        public int compare(final Map<String, Comparable<?>> p1, final Map<String, Comparable<?>> p2) {
            final int p1Size = Objects.requireNonNull(p1).size();
            final int p2Size = Objects.requireNonNull(p2).size();
            checkSizeMismatch(p1, p1Size, p2, p2Size);
            checkSizeMismatch(p2, p2Size, p1, p1Size);

            for (final Map.Entry<String, Comparable<?>> p1Entry : p1.entrySet()) {
                final String partitionKey = p1Entry.getKey();
                final Comparable<?> p1Value = p1Entry.getValue();
                final Comparable<?> p2Value = p2.getOrDefault(partitionKey, MISSING_PARTITION_VALUE);
                if (p2Value == MISSING_PARTITION_VALUE) {
                    throw new UnknownPartitionKeyException(partitionKey);
                }
                final int comparisonResult = ObjectComparisons.compare(p1Value, p2Value);
                if (comparisonResult != 0) {
                    return comparisonResult;
                }
            }

            return 0;
        }
    }
}
