package io.deephaven.db.v2.locations.local;

import io.deephaven.base.log.LogOutput;
import io.deephaven.db.v2.locations.ImmutableTableLocationKey;
import io.deephaven.db.v2.locations.impl.PartitionedTableLocationKey;
import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.util.Map;

/**
 * Base {@link ImmutableTableLocationKey} implementation for table locations that may be enclosed by partitions and
 * described by a {@link File}. Sub-classes should override {@link #compareTo(TableLocationKey)} and
 * {@link #equals(Object)} only if they need to prevent equality with other {@link FileTableLocationKey}
 * implementations.
 */
public class FileTableLocationKey extends PartitionedTableLocationKey {

    private static final String IMPLEMENTATION_NAME = FileTableLocationKey.class.getSimpleName();

    protected final File file;
    private final int order;

    private int cachedHashCode;

    /**
     * Construct a new FileTableLocationKey for the supplied {@code file} and {@code partitions}.
     *
     * @param file The file (or directory) that backs the keyed location. Will be adjusted to an absolute path.
     * @param order Explicit ordering value for this location key. {@link Comparable#compareTo(Object)} will sort
     *        FileTableLocationKeys with a lower {@code order} before other keys. Comparing this ordering value takes
     *        precedence over other fields.
     * @param partitions The table partitions enclosing the table location keyed by {@code this}. Note that if this
     *        parameter is {@code null}, the location will be a member of no partitions. An ordered copy of the map will
     *        be made, so the calling code is free to mutate the map after this call completes, but the partition keys
     *        and values themselves <em>must</em> be effectively immutable.
     */
    public FileTableLocationKey(@NotNull final File file, final int order,
            @Nullable final Map<String, Comparable<?>> partitions) {
        super(partitions);
        this.file = file.getAbsoluteFile();
        this.order = order;
    }

    public final File getFile() {
        return file;
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

    /**
     * Precedence-wise this implementation compares {@code order}, then applies a {@link PartitionsComparator} to
     * {@code partitions}, then compares {@code file}.
     * 
     * @inheritDoc
     */
    @Override
    public int compareTo(@NotNull final TableLocationKey other) {
        if (other instanceof FileTableLocationKey) {
            final FileTableLocationKey otherTyped = (FileTableLocationKey) other;
            final int orderingComparisonResult = Integer.compare(order, otherTyped.order);
            if (orderingComparisonResult != 0) {
                return orderingComparisonResult;
            }
            final int partitionComparisonResult =
                    PartitionsComparator.INSTANCE.compare(partitions, otherTyped.partitions);
            if (partitionComparisonResult != 0) {
                return partitionComparisonResult;
            }
            return file.compareTo(otherTyped.file);
        }
        throw new ClassCastException("Cannot compare " + getClass() + " to " + other.getClass());
    }

    @Override
    public int hashCode() {
        if (cachedHashCode == 0) {
            final int computedHashCode = 31 * partitions.hashCode() + file.hashCode();
            // Don't use 0; that's used by StandaloneTableLocationKey, and also our sentinel for the need to compute
            if (computedHashCode == 0) {
                final int fallbackHashCode = FileTableLocationKey.class.hashCode();
                cachedHashCode = fallbackHashCode == 0 ? 1 : fallbackHashCode;
            } else {
                cachedHashCode = computedHashCode;
            }
        }
        return cachedHashCode;
    }

    @Override
    public boolean equals(@Nullable final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof FileTableLocationKey)) {
            return false;
        }
        final FileTableLocationKey otherTyped = (FileTableLocationKey) other;
        return file.equals(otherTyped.file) && partitions.equals(otherTyped.partitions);
    }

    @Override
    public String getImplementationName() {
        return IMPLEMENTATION_NAME;
    }
}
