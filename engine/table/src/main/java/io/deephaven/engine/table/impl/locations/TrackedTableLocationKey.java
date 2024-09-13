//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.ReferenceCountedLivenessNode;

import java.util.function.Consumer;

/**
 * Sub-interface of {@link TableLocationKey} to mark immutable implementations.
 */
public class TrackedTableLocationKey extends ReferenceCountedLivenessNode {

    public static TrackedTableLocationKey[] ZERO_LENGTH_TRACKED_TABLE_LOCATION_KEY_ARRAY =
            new TrackedTableLocationKey[0];

    final ImmutableTableLocationKey locationKey;
    final Consumer<TrackedTableLocationKey> zeroCountConsumer;

    private TableLocation tableLocation;

    public TrackedTableLocationKey(
            final ImmutableTableLocationKey locationKey,
            final Consumer<TrackedTableLocationKey> zeroCountConsumer) {
        super(false);

        this.locationKey = locationKey;
        this.zeroCountConsumer = zeroCountConsumer;

        tableLocation = null;
    }

    public ImmutableTableLocationKey getKey() {
        return locationKey;
    }

    @Override
    public int hashCode() {
        return locationKey.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ImmutableTableLocationKey) {
            return locationKey.equals(obj);
        }
        return (this == obj);
    }

    /**
     * This {@link TrackedTableLocationKey} should manage the given {@link TableLocation} and store a reference to it.
     * 
     * @param tableLocation The {@link TableLocation} to manage.
     */
    public void manageTableLocation(TableLocation tableLocation) {
        Assert.eqNull(this.tableLocation, "this.tableLocation");
        this.tableLocation = tableLocation;
        manage(tableLocation);
    }

    /**
     * Unmanage the {@link TableLocation} and the release the reference.
     */
    public void unmanageTableLocation() {
        Assert.neqNull(this.tableLocation, "this.tableLocation");
        unmanage(tableLocation);
        tableLocation = null;
    }

    @Override
    protected void destroy() {
        super.destroy();
        if (tableLocation != null) {
            unmanageTableLocation();
        }
        zeroCountConsumer.accept(this);
    }
}
