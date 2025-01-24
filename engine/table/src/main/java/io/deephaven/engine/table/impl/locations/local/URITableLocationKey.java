//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit FileTableLocationKey and run "./gradlew replicateTableLocationKey" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.locations.local;

import io.deephaven.base.log.LogOutput;
import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import io.deephaven.engine.table.impl.locations.impl.PartitionedTableLocationKey;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.util.Map;

/**
 * Base {@link ImmutableTableLocationKey} implementation for table locations that may be enclosed by partitions and
 * described by a {@link URI}. Sub-classes should override {@link #compareTo(TableLocationKey)} and
 * {@link #equals(Object)} only if they need to prevent equality with other {@link URITableLocationKey} implementations.
 */
public class URITableLocationKey extends PartitionedTableLocationKey {

    private static final String IMPLEMENTATION_NAME = URITableLocationKey.class.getSimpleName();

    protected final URI uri;
    protected final int order;

    /**
     * Construct a new URITableLocationKey for the supplied {@code uri} and {@code partitions}.
     *
     * @param uri The uri (or directory) that backs the keyed location. Will be adjusted to an absolute path.
     * @param order Explicit ordering value for this location key. {@link Comparable#compareTo(Object)} will sort
     *        URITableLocationKeys with a lower {@code order} before other keys. Comparing this ordering value takes
     *        precedence over other fields.
     * @param partitions The table partitions enclosing the table location keyed by {@code this}. Note that if this
     *        parameter is {@code null}, the location will be a member of no partitions. An ordered copy of the map will
     *        be made, so the calling code is free to mutate the map after this call completes, but the partition keys
     *        and values themselves <em>must</em> be effectively immutable.
     */
    public URITableLocationKey(@NotNull final URI uri, final int order,
            @Nullable final Map<String, Comparable<?>> partitions) {
        super(partitions);
        if (!uri.isAbsolute()) {
            throw new IllegalArgumentException("URI must be absolute");
        }
        this.uri = uri;
        this.order = order;
    }

    public final URI getURI() {
        return uri;
    }

    @Override
    public LogOutput append(@NotNull final LogOutput logOutput) {
        return logOutput.append(getImplementationName())
                .append(":[uri=").append(uri.getPath())
                .append(",partitions=").append(PartitionsFormatter.INSTANCE, partitions)
                .append(']');
    }

    @Override
    public String toString() {
        return new LogOutputStringImpl().append(this).toString();
    }

    /**
     * When comparing with another {@link URITableLocationKey}, precedence-wise this implementation compares
     * {@code order}, then applies a {@link PartitionsComparator} to {@code partitions}, then compares {@code uri}.
     * Otherwise, it delegates to parent class.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public int compareTo(@NotNull final TableLocationKey other) {
        if (other instanceof URITableLocationKey) {
            final URITableLocationKey otherTyped = (URITableLocationKey) other;
            final int orderingComparisonResult = Integer.compare(order, otherTyped.order);
            if (orderingComparisonResult != 0) {
                return orderingComparisonResult;
            }
            final int partitionComparisonResult =
                    PartitionsComparator.INSTANCE.compare(partitions, otherTyped.partitions);
            if (partitionComparisonResult != 0) {
                return partitionComparisonResult;
            }
            return uri.compareTo(otherTyped.uri);
        }
        return super.compareTo(other);
    }

    @Override
    public int hashCode() {
        if (cachedHashCode == 0) {
            final int computedHashCode = 31 * partitions.hashCode() + uri.hashCode();
            // Don't use 0; that's used by StandaloneTableLocationKey, and also our sentinel for the need to compute
            if (computedHashCode == 0) {
                final int fallbackHashCode = URITableLocationKey.class.hashCode();
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
        if (!(other instanceof URITableLocationKey)) {
            return false;
        }
        final URITableLocationKey otherTyped = (URITableLocationKey) other;
        return uri.equals(otherTyped.uri) && partitions.equals(otherTyped.partitions);
    }

    @Override
    public String getImplementationName() {
        return IMPLEMENTATION_NAME;
    }
}
