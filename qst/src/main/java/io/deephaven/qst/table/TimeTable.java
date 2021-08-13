package io.deephaven.qst.table;

import io.deephaven.annotations.LeafStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * A time table adds rows at a fixed {@link #interval() interval} with a
 * {@link io.deephaven.qst.type.InstantType Timestamp} column.
 */
@Immutable
@LeafStyle
public abstract class TimeTable extends TableBase {

    /**
     * Used when constructing a time table whose construction is specific enough to be memoizable.
     */
    private static final UUID ZERO_UUID = UUID.fromString("00000000-0000-0000-0000-000000000000");

    public static Builder builder() {
        return ImmutableTimeTable.builder();
    }

    /**
     * The time table.
     *
     * <p>
     * Note: {@code !TimeTable.of(interval).equals(TimeTable.of(interval))}.
     * 
     * @param interval the interval
     * @return the time table
     */
    public static TimeTable of(Duration interval) {
        return builder().timeProvider(TimeProviderSystem.INSTANCE).interval(interval)
            .id(UUID.randomUUID()).build();
    }

    /**
     * The time table.
     *
     * @param interval the interval
     * @param startTime the start time
     * @return the time table
     */
    public static TimeTable of(Duration interval, Instant startTime) {
        return builder().timeProvider(TimeProviderSystem.INSTANCE).interval(interval)
            .startTime(startTime).id(ZERO_UUID).build();
    }

    // Note: if new "of(...)" static methods are added here, they should likely be added to
    // TableCreator.

    public abstract TimeProvider timeProvider();

    public abstract Duration interval();

    public abstract Optional<Instant> startTime();

    abstract UUID id();

    @Override
    public final <V extends TableSpec.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkTimeout() {
        if (interval().isNegative() || interval().isZero()) {
            throw new IllegalArgumentException("Must have positive interval");
        }
    }

    interface Builder {
        Builder timeProvider(TimeProvider timeProvider);

        Builder interval(Duration interval);

        Builder startTime(Instant startTime);

        Builder id(UUID id);

        TimeTable build();
    }
}
