//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import io.deephaven.annotations.LeafStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * A time table adds rows at a fixed {@link #interval() interval} with a {@link io.deephaven.qst.type.InstantType
 * Timestamp} column.
 */
@Immutable
@LeafStyle
public abstract class TimeTable extends TableBase {

    /**
     * Used when constructing a time table whose construction is specific enough to be memoizable.
     */
    private static final Object INTERNAL_MEMO = new Object();

    /**
     * Create a time table builder.
     *
     * <p>
     * Time tables constructed via this builder are not equal to other instances unless all the fields specified are
     * equal <b>and</b> {@link Builder#id(Object)} is explicitly set to equivalent objects.
     *
     * @return the builder
     */
    public static Builder builder() {
        return ImmutableTimeTable.builder();
    }

    /**
     * The time table.
     *
     * <p>
     * Note: {@code of(interval).equals(of(interval)) == false}.
     * 
     * @param interval the interval
     * @return the time table
     */
    public static TimeTable of(Duration interval) {
        return builder().interval(interval).build();
    }

    /**
     * The time table. Instances constructed via this method with the same {@code interval} and {@code startTime} will
     * be equal.
     *
     * @param interval the interval
     * @param startTime the start time
     * @return the time table
     */
    public static TimeTable of(Duration interval, Instant startTime) {
        return builder()
                .interval(interval)
                .startTime(startTime)
                .id(INTERNAL_MEMO)
                .build();
    }

    // Note: if new "of(...)" static methods are added here, they should likely be added to
    // TableCreator.

    @Default
    public Clock clock() {
        return ClockSystem.INSTANCE;
    }

    public abstract Duration interval();

    public abstract Optional<Instant> startTime();

    @Default
    public boolean blinkTable() {
        return false;
    }

    @Default
    Object id() {
        return new Object();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    @Check
    final void checkTimeout() {
        if (interval().isNegative() || interval().isZero()) {
            throw new IllegalArgumentException("Must have positive interval");
        }
    }

    public interface Builder {
        Builder clock(Clock clock);

        Builder interval(Duration interval);

        Builder startTime(Instant startTime);

        Builder blinkTable(boolean blinkTable);

        Builder id(Object id);

        TimeTable build();
    }
}
