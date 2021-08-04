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
     * Note: {@code !TimeTable.of(timeout).equals(TimeTable.of(timeout))}.
     * 
     * @param timeout the timeout
     * @return the time table
     */
    public static TimeTable of(Duration timeout) {
        return builder().timeProvider(TimeProviderSystem.INSTANCE).interval(timeout)
            .id(UUID.randomUUID()).build();
    }

    /**
     * The time table.
     *
     * @param timeout the timeout
     * @param startTime the start time
     * @return the time table
     */
    public static TimeTable of(Duration timeout, Instant startTime) {
        return builder().timeProvider(TimeProviderSystem.INSTANCE).interval(timeout)
            .startTime(startTime).id(ZERO_UUID).build();
    }

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
