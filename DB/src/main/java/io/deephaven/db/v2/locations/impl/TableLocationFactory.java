package io.deephaven.db.v2.locations.impl;

import io.deephaven.db.v2.locations.TableKey;
import io.deephaven.db.v2.locations.TableLocation;
import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.db.v2.locations.TableLocationProvider;
import io.deephaven.db.v2.locations.util.TableDataRefreshService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Interface for {@link TableLocation} creation.
 */
public interface TableLocationFactory<TK extends TableKey, TLK extends TableLocationKey> {

    /**
     * Manufacture a {@link TableLocation} from the supplied key
     *
     * @param tableKey       The {@link TableKey} for the provider using this factory
     * @param locationKey    The {@link TableLocationKey} (or an immutable equivalent) returned by a paired
     *                       {@link TableLocationKeyFinder}
     * @param refreshService An optional {@link TableDataRefreshService}, non-null if the enclosing provider
     *                       {@link TableLocationProvider#supportsSubscriptions() supports subscriptions}
     * @return A new or cached {@link TableLocation} identified by the supplied {@link TableKey} and {@link TableLocationKey}
     */
    TableLocation makeLocation(@NotNull TK tableKey, @NotNull TLK locationKey, @Nullable TableDataRefreshService refreshService);
}
