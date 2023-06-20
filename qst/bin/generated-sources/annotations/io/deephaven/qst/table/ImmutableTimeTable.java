package io.deephaven.qst.table;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link TimeTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableTimeTable.builder()}.
 */
@Generated(from = "TimeTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableTimeTable extends TimeTable {
  private transient final int depth;
  private final Clock clock;
  private final Duration interval;
  private final Instant startTime;
  private final UUID id;

  private ImmutableTimeTable(ImmutableTimeTable.Builder builder) {
    this.clock = builder.clock;
    this.interval = builder.interval;
    this.startTime = builder.startTime;
    this.id = builder.id;
    this.depth = super.depth();
  }

  /**
   * @return The computed-at-construction value of the {@code depth} attribute
   */
  @Override
  public int depth() {
    return depth;
  }

  /**
   * @return The value of the {@code clock} attribute
   */
  @Override
  public Clock clock() {
    return clock;
  }

  /**
   * @return The value of the {@code interval} attribute
   */
  @Override
  public Duration interval() {
    return interval;
  }

  /**
   * @return The value of the {@code startTime} attribute
   */
  @Override
  public Optional<Instant> startTime() {
    return Optional.ofNullable(startTime);
  }

  /**
   * @return The value of the {@code id} attribute
   */
  @Override
  UUID id() {
    return id;
  }

  /**
   * This instance is equal to all instances of {@code ImmutableTimeTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableTimeTable
        && equalTo(0, (ImmutableTimeTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableTimeTable another) {
    return depth == another.depth
        && clock.equals(another.clock)
        && interval.equals(another.interval)
        && Objects.equals(startTime, another.startTime)
        && id.equals(another.id);
  }

  /**
   * Computes a hash code from attributes: {@code depth}, {@code clock}, {@code interval}, {@code startTime}, {@code id}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + getClass().hashCode();
    h += (h << 5) + depth;
    h += (h << 5) + clock.hashCode();
    h += (h << 5) + interval.hashCode();
    h += (h << 5) + Objects.hashCode(startTime);
    h += (h << 5) + id.hashCode();
    return h;
  }

  private static ImmutableTimeTable validate(ImmutableTimeTable instance) {
    instance.checkTimeout();
    return instance;
  }

  /**
   * Creates a builder for {@link ImmutableTimeTable ImmutableTimeTable}.
   * <pre>
   * ImmutableTimeTable.builder()
   *    .clock(io.deephaven.qst.table.Clock) // required {@link TimeTable#clock() clock}
   *    .interval(java.time.Duration) // required {@link TimeTable#interval() interval}
   *    .startTime(java.time.Instant) // optional {@link TimeTable#startTime() startTime}
   *    .id(UUID) // required {@link TimeTable#id() id}
   *    .build();
   * </pre>
   * @return A new ImmutableTimeTable builder
   */
  public static ImmutableTimeTable.Builder builder() {
    return new ImmutableTimeTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableTimeTable ImmutableTimeTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "TimeTable", generator = "Immutables")
  public static final class Builder implements TimeTable.Builder {
    private static final long INIT_BIT_CLOCK = 0x1L;
    private static final long INIT_BIT_INTERVAL = 0x2L;
    private static final long INIT_BIT_ID = 0x4L;
    private static final long OPT_BIT_START_TIME = 0x1L;
    private long initBits = 0x7L;
    private long optBits;

    private Clock clock;
    private Duration interval;
    private Instant startTime;
    private UUID id;

    private Builder() {
    }

    /**
     * Initializes the value for the {@link TimeTable#clock() clock} attribute.
     * @param clock The value for clock 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder clock(Clock clock) {
      checkNotIsSet(clockIsSet(), "clock");
      this.clock = Objects.requireNonNull(clock, "clock");
      initBits &= ~INIT_BIT_CLOCK;
      return this;
    }

    /**
     * Initializes the value for the {@link TimeTable#interval() interval} attribute.
     * @param interval The value for interval 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder interval(Duration interval) {
      checkNotIsSet(intervalIsSet(), "interval");
      this.interval = Objects.requireNonNull(interval, "interval");
      initBits &= ~INIT_BIT_INTERVAL;
      return this;
    }

    /**
     * Initializes the optional value {@link TimeTable#startTime() startTime} to startTime.
     * @param startTime The value for startTime
     * @return {@code this} builder for chained invocation
     */
    public final Builder startTime(Instant startTime) {
      checkNotIsSet(startTimeIsSet(), "startTime");
      this.startTime = Objects.requireNonNull(startTime, "startTime");
      optBits |= OPT_BIT_START_TIME;
      return this;
    }

    /**
     * Initializes the optional value {@link TimeTable#startTime() startTime} to startTime.
     * @param startTime The value for startTime
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder startTime(Optional<? extends Instant> startTime) {
      checkNotIsSet(startTimeIsSet(), "startTime");
      this.startTime = startTime.orElse(null);
      optBits |= OPT_BIT_START_TIME;
      return this;
    }

    /**
     * Initializes the value for the {@link TimeTable#id() id} attribute.
     * @param id The value for id 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder id(UUID id) {
      checkNotIsSet(idIsSet(), "id");
      this.id = Objects.requireNonNull(id, "id");
      initBits &= ~INIT_BIT_ID;
      return this;
    }

    /**
     * Builds a new {@link ImmutableTimeTable ImmutableTimeTable}.
     * @return An immutable instance of TimeTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableTimeTable build() {
      checkRequiredAttributes();
      return ImmutableTimeTable.validate(new ImmutableTimeTable(this));
    }

    private boolean startTimeIsSet() {
      return (optBits & OPT_BIT_START_TIME) != 0;
    }

    private boolean clockIsSet() {
      return (initBits & INIT_BIT_CLOCK) == 0;
    }

    private boolean intervalIsSet() {
      return (initBits & INIT_BIT_INTERVAL) == 0;
    }

    private boolean idIsSet() {
      return (initBits & INIT_BIT_ID) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of TimeTable is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!clockIsSet()) attributes.add("clock");
      if (!intervalIsSet()) attributes.add("interval");
      if (!idIsSet()) attributes.add("id");
      return "Cannot build TimeTable, some of required attributes are not set " + attributes;
    }
  }
}
