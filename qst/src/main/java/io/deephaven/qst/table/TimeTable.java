package io.deephaven.qst.table;

import io.deephaven.qst.LeafStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

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
        return builder().timeProvider(TimeProviderSystem.INSTANCE).timeout(timeout)
            .memoizationBuster(UUID.randomUUID()).build();
    }

    /**
     * The time table.
     *
     * @param timeout the timeout
     * @param startTime the start time
     * @return the time table
     */
    public static TimeTable of(Duration timeout, Instant startTime) {
        return builder().timeProvider(TimeProviderSystem.INSTANCE).timeout(timeout)
            .startTime(startTime).memoizationBuster(ZERO_UUID).build();
    }

    public abstract TimeProvider timeProvider();

    public abstract Duration timeout();

    public abstract Optional<Instant> startTime();

    abstract UUID memoizationBuster();

    @Override
    public final <V extends Table.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkTimeout() {
        if (timeout().isNegative() || timeout().isZero()) {
            throw new IllegalArgumentException("Must have positive timeout");
        }
    }

    interface Builder {
        Builder timeProvider(TimeProvider timeProvider);

        Builder timeout(Duration timeout);

        Builder startTime(Instant startTime);

        Builder memoizationBuster(UUID memoizationBuster);

        TimeTable build();
    }
}
