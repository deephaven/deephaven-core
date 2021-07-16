package io.deephaven.db.v2.locations.parquet;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.db.util.DhObjectComparisons;
import io.deephaven.db.v2.locations.ImmutableTableLocationKey;
import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.db.v2.locations.UnknownPartitionKeyException;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.util.*;

/**
 * {@link TableLocationKey} implementation for use with data stored in the parquet format.
 */
public final class ParquetTableLocationKey implements ImmutableTableLocationKey {

    private static final String IMPLEMENTATION_NAME = ParquetTableLocationKey.class.getSimpleName();
    @SuppressWarnings("StringOperationCanBeSimplified")
    private static final Comparable<?> MISSING_PARTITION_VALUE = new String("MISSING PARTITION VALUE");

    private final Map<String, Comparable<?>> partitions;
    private final File file;

    /**
     * Construct a new ParquetTableLocationKey for the supplied {@code file} and {@code partitions}.
     *
     * @param file       The parquet file that backs the keyed location. Will be adjusted to
     * @param partitions The table partitions enclosing the table location keyed by {@code this}. Note that if this
     *                   parameter is {@code null}, the location will be a member of no partitions. An ordered copy
     *                   of the map will be made, so the calling code is free to mutate the map after this call
     *                   completes, but the partition keys and values themselves <em>must</em> be effectively immutable.
     */
    public ParquetTableLocationKey(@NotNull final File file, @Nullable final Map<String, Comparable<?>> partitions) {
        this.file = file.getAbsoluteFile();
        this.partitions = partitions == null || partitions.isEmpty() ? Collections.emptyMap() : Collections.unmodifiableMap(new LinkedHashMap<>(partitions));
    }

    @Override
    public String getImplementationName() {
        return IMPLEMENTATION_NAME;
    }

    @Override
    public LogOutput append(@NotNull final LogOutput logOutput) {
        return logOutput.append(getImplementationName())
                .append(":[file=").append(file.getPath())
                .append(",partitions=").append(PartitionsFormatter.INSTANCE, partitions)
                .append(']');
    }

    @Override
    public String toString() {
        return new LogOutputStringImpl().append(this).toString();
    }

    @Override
    public <PARTITION_VALUE_TYPE> PARTITION_VALUE_TYPE getPartitionValue(@NotNull final String partitionKey) {
        final Object partitionValue = partitions.getOrDefault(partitionKey, MISSING_PARTITION_VALUE);
        if (partitionValue == MISSING_PARTITION_VALUE) {
            throw new UnknownPartitionKeyException(partitionKey, this);
        }
        //noinspection unchecked
        return (PARTITION_VALUE_TYPE) partitionValue;
    }

    @Override
    public int compareTo(@NotNull final TableLocationKey other) {
        if (other instanceof ParquetTableLocationKey) {
            final ParquetTableLocationKey otherTyped = (ParquetTableLocationKey) other;
            final int partitionComparisonResult = PartitionsComparator.INSTANCE.compare(partitions, otherTyped.partitions);
            return partitionComparisonResult != 0 ? partitionComparisonResult : file.compareTo(otherTyped.file);
        }
        throw new ClassCastException("Cannot compare " + getClass() + " to " + other.getClass());
    }

    @Override
    public int hashCode() {
        return 31 * partitions.hashCode() + file.hashCode();
    }

    @Override
    public boolean equals(@Nullable final Object other) {
        //TODO
    }

    /**
     * Formats a map of partitions as key-value pairs.
     */
    private static final class PartitionsFormatter implements LogOutput.ObjFormatter<Map<String, Comparable<?>>> {

        private static final LogOutput.ObjFormatter<Map<String, Comparable<?>>> INSTANCE = new PartitionsFormatter();

        private PartitionsFormatter() {
        }

        @Override
        public void format(@NotNull final LogOutput logOutput, @NotNull final Map<String, Comparable<?>> partitions) {
            if (partitions.isEmpty()) {
                logOutput.append("{}");
                return;
            }

            logOutput.append('{');

            for (final Iterator<Map.Entry<String, Comparable<?>>> pi = partitions.entrySet().iterator(); pi.hasNext(); ) {
                final Map.Entry<String, Comparable<?>> partition = pi.next();
                final String partitionKey = partition.getKey();
                final Comparable<?> partitionValue = partition.getValue();
                logOutput.append(partitionKey).append('=');

                if (partitionValue == null) {
                    logOutput.append("null");
                } else if (partitionValue instanceof CharSequence) {
                    logOutput.append((CharSequence) partitionValue);
                } else if (partitionValue instanceof Long || partitionValue instanceof Integer || partitionValue instanceof Short || partitionValue instanceof Byte) {
                    logOutput.append(((Number) partitionValue).longValue());
                } else if (partitionValue instanceof Double || partitionValue instanceof Float) {
                    logOutput.appendDouble(((Number) partitionValue).doubleValue());
                } else if (partitionValue instanceof Character) {
                    logOutput.appendDouble((char) partitionValue);
                } else if (partitionValue instanceof LogOutputAppendable) {
                    logOutput.append((LogOutputAppendable) partitionValue);
                } else {
                    // Boolean, DBDateTime, or some other arbitrary object we haven't considered
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
    private static final class PartitionsComparator implements Comparator<Map<String, Comparable<?>>> {

        private static final Comparator<Map<String, Comparable<?>>> INSTANCE = new PartitionsComparator();

        private PartitionsComparator() {
        }

        @Override
        public int compare(final Map<String, Comparable<?>> p1, final Map<String, Comparable<?>> p2) {
            final int p1Size = Objects.requireNonNull(p1).size();
            final int p2Size = Objects.requireNonNull(p2).size();
            if (p1Size > p2Size) {
                //noinspection OptionalGetWithoutIsPresent
                throw new UnknownPartitionKeyException(p1.keySet().stream().filter(pk -> !p2.containsKey(pk)).findFirst().get());
            }
            if (p2Size > p1Size) {
                //noinspection OptionalGetWithoutIsPresent
                throw new UnknownPartitionKeyException(p2.keySet().stream().filter(pk -> !p1.containsKey(pk)).findFirst().get());
            }

            for (final Map.Entry<String, Comparable<?>> p1Entry : p1.entrySet()) {
                final String partitionKey = p1Entry.getKey();
                final Comparable<?> p1Value = p1Entry.getValue();
                final Comparable<?> p2Value = p2.getOrDefault(partitionKey, MISSING_PARTITION_VALUE);
                if (p2Value == MISSING_PARTITION_VALUE) {
                    throw new UnknownPartitionKeyException(partitionKey);
                }
                final int comparisonResult = DhObjectComparisons.compare(p1Value, p2Value);
                if (comparisonResult != 0) {
                    return comparisonResult;
                }
            }

            return 0;
        }
    }
}
