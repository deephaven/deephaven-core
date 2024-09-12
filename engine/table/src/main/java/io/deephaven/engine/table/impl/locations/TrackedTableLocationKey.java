//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.ReferenceCountedLivenessNode;
import org.jetbrains.annotations.NotNull;

import java.util.Set;
import java.util.function.Consumer;

/**
 * Sub-interface of {@link TableLocationKey} to mark immutable implementations.
 */
public class TrackedTableLocationKey extends ReferenceCountedLivenessNode implements ImmutableTableLocationKey {

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

    @Override
    public <PARTITION_VALUE_TYPE extends Comparable<PARTITION_VALUE_TYPE>> PARTITION_VALUE_TYPE getPartitionValue(
            @NotNull String partitionKey) {
        return locationKey.getPartitionValue(partitionKey);
    }

    @Override
    public Set<String> getPartitionKeys() {
        return locationKey.getPartitionKeys();
    }

    @Override
    public int compareTo(@NotNull TableLocationKey o) {
        return locationKey.compareTo(o);
    }

    @Override
    public boolean equals(Object o) {
        return locationKey.equals(o);
    }
}
