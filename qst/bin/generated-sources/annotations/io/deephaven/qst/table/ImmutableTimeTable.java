package io.deephaven.qst.table;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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
<<<<<<< HEAD
final class ImmutableTimeTable extends TimeTable {
=======
public final class ImmutableTimeTable extends TimeTable {
>>>>>>> main
  private transient final int depth;
  private final Clock clock;
  private final Duration interval;
  private final Instant startTime;
  private final boolean blinkTable;
  private final Object id;

  private ImmutableTimeTable(ImmutableTimeTable.Builder builder) {
    this.interval = builder.interval;
    this.startTime = builder.startTime;
<<<<<<< HEAD
    if (builder.clockIsSet()) {
=======
    if (builder.clock != null) {
>>>>>>> main
      initShim.clock(builder.clock);
    }
    if (builder.blinkTableIsSet()) {
      initShim.blinkTable(builder.blinkTable);
    }
<<<<<<< HEAD
    if (builder.idIsSet()) {
=======
    if (builder.id != null) {
>>>>>>> main
      initShim.id(builder.id);
    }
    this.depth = initShim.depth();
    this.clock = initShim.clock();
    this.blinkTable = initShim.blinkTable();
    this.id = initShim.id();
    this.initShim = null;
  }

<<<<<<< HEAD
=======
  private ImmutableTimeTable(
      Clock clock,
      Duration interval,
      Instant startTime,
      boolean blinkTable,
      Object id) {
    initShim.clock(clock);
    this.interval = interval;
    this.startTime = startTime;
    initShim.blinkTable(blinkTable);
    initShim.id(id);
    this.depth = initShim.depth();
    this.clock = initShim.clock();
    this.blinkTable = initShim.blinkTable();
    this.id = initShim.id();
    this.initShim = null;
  }

>>>>>>> main
  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "TimeTable", generator = "Immutables")
  private final class InitShim {
    private byte depthBuildStage = STAGE_UNINITIALIZED;
    private int depth;

    int depth() {
      if (depthBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (depthBuildStage == STAGE_UNINITIALIZED) {
        depthBuildStage = STAGE_INITIALIZING;
        this.depth = ImmutableTimeTable.super.depth();
        depthBuildStage = STAGE_INITIALIZED;
      }
      return this.depth;
    }

    private byte clockBuildStage = STAGE_UNINITIALIZED;
    private Clock clock;

    Clock clock() {
      if (clockBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (clockBuildStage == STAGE_UNINITIALIZED) {
        clockBuildStage = STAGE_INITIALIZING;
        this.clock = Objects.requireNonNull(ImmutableTimeTable.super.clock(), "clock");
        clockBuildStage = STAGE_INITIALIZED;
      }
      return this.clock;
    }

    void clock(Clock clock) {
      this.clock = clock;
      clockBuildStage = STAGE_INITIALIZED;
    }

    private byte blinkTableBuildStage = STAGE_UNINITIALIZED;
    private boolean blinkTable;

    boolean blinkTable() {
      if (blinkTableBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (blinkTableBuildStage == STAGE_UNINITIALIZED) {
        blinkTableBuildStage = STAGE_INITIALIZING;
        this.blinkTable = ImmutableTimeTable.super.blinkTable();
        blinkTableBuildStage = STAGE_INITIALIZED;
      }
      return this.blinkTable;
    }

    void blinkTable(boolean blinkTable) {
      this.blinkTable = blinkTable;
      blinkTableBuildStage = STAGE_INITIALIZED;
    }

    private byte idBuildStage = STAGE_UNINITIALIZED;
    private Object id;

    Object id() {
      if (idBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (idBuildStage == STAGE_UNINITIALIZED) {
        idBuildStage = STAGE_INITIALIZING;
        this.id = Objects.requireNonNull(ImmutableTimeTable.super.id(), "id");
        idBuildStage = STAGE_INITIALIZED;
      }
      return this.id;
    }

    void id(Object id) {
      this.id = id;
      idBuildStage = STAGE_INITIALIZED;
    }

    private String formatInitCycleMessage() {
      List<String> attributes = new ArrayList<>();
      if (depthBuildStage == STAGE_INITIALIZING) attributes.add("depth");
      if (clockBuildStage == STAGE_INITIALIZING) attributes.add("clock");
      if (blinkTableBuildStage == STAGE_INITIALIZING) attributes.add("blinkTable");
      if (idBuildStage == STAGE_INITIALIZING) attributes.add("id");
      return "Cannot build TimeTable, attribute initializers form cycle " + attributes;
    }
  }

  /**
<<<<<<< HEAD
   * @return The computed-at-construction value of the {@code depth} attribute
=======
   * The depth of the table is the maximum depth of its dependencies plus one. A table with no dependencies has a
   * depth of zero.
   * @return the depth
>>>>>>> main
   */
  @Override
  public int depth() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.depth()
        : this.depth;
  }

  /**
   * @return The value of the {@code clock} attribute
   */
  @Override
  public Clock clock() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.clock()
        : this.clock;
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
   * @return The value of the {@code blinkTable} attribute
   */
  @Override
  public boolean blinkTable() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.blinkTable()
        : this.blinkTable;
  }

  /**
   * @return The value of the {@code id} attribute
   */
  @Override
  Object id() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.id()
        : this.id;
  }

  /**
<<<<<<< HEAD
=======
   * Copy the current immutable object by setting a value for the {@link TimeTable#clock() clock} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for clock
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTimeTable withClock(Clock value) {
    if (this.clock == value) return this;
    Clock newValue = Objects.requireNonNull(value, "clock");
    return validate(new ImmutableTimeTable(newValue, this.interval, this.startTime, this.blinkTable, this.id));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TimeTable#interval() interval} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for interval
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTimeTable withInterval(Duration value) {
    if (this.interval == value) return this;
    Duration newValue = Objects.requireNonNull(value, "interval");
    return validate(new ImmutableTimeTable(this.clock, newValue, this.startTime, this.blinkTable, this.id));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link TimeTable#startTime() startTime} attribute.
   * @param value The value for startTime
   * @return A modified copy of {@code this} object
   */
  public final ImmutableTimeTable withStartTime(Instant value) {
    Instant newValue = Objects.requireNonNull(value, "startTime");
    if (this.startTime == newValue) return this;
    return validate(new ImmutableTimeTable(this.clock, this.interval, newValue, this.blinkTable, this.id));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link TimeTable#startTime() startTime} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for startTime
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableTimeTable withStartTime(Optional<? extends Instant> optional) {
    Instant value = optional.orElse(null);
    if (this.startTime == value) return this;
    return validate(new ImmutableTimeTable(this.clock, this.interval, value, this.blinkTable, this.id));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TimeTable#blinkTable() blinkTable} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for blinkTable
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTimeTable withBlinkTable(boolean value) {
    if (this.blinkTable == value) return this;
    return validate(new ImmutableTimeTable(this.clock, this.interval, this.startTime, value, this.id));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TimeTable#id() id} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for id
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTimeTable withId(Object value) {
    if (this.id == value) return this;
    Object newValue = Objects.requireNonNull(value, "id");
    return validate(new ImmutableTimeTable(this.clock, this.interval, this.startTime, this.blinkTable, newValue));
  }

  /**
>>>>>>> main
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
        && blinkTable == another.blinkTable
        && id.equals(another.id);
  }

  /**
   * Computes a hash code from attributes: {@code depth}, {@code clock}, {@code interval}, {@code startTime}, {@code blinkTable}, {@code id}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
<<<<<<< HEAD
    h += (h << 5) + getClass().hashCode();
=======
>>>>>>> main
    h += (h << 5) + depth;
    h += (h << 5) + clock.hashCode();
    h += (h << 5) + interval.hashCode();
    h += (h << 5) + Objects.hashCode(startTime);
    h += (h << 5) + Boolean.hashCode(blinkTable);
    h += (h << 5) + id.hashCode();
    return h;
  }

  private static ImmutableTimeTable validate(ImmutableTimeTable instance) {
    instance.checkTimeout();
    return instance;
  }

  /**
<<<<<<< HEAD
=======
   * Creates an immutable copy of a {@link TimeTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable TimeTable instance
   */
  public static ImmutableTimeTable copyOf(TimeTable instance) {
    if (instance instanceof ImmutableTimeTable) {
      return (ImmutableTimeTable) instance;
    }
    return ImmutableTimeTable.builder()
        .from(instance)
        .build();
  }

  /**
>>>>>>> main
   * Creates a builder for {@link ImmutableTimeTable ImmutableTimeTable}.
   * <pre>
   * ImmutableTimeTable.builder()
   *    .clock(io.deephaven.qst.table.Clock) // optional {@link TimeTable#clock() clock}
   *    .interval(java.time.Duration) // required {@link TimeTable#interval() interval}
   *    .startTime(java.time.Instant) // optional {@link TimeTable#startTime() startTime}
   *    .blinkTable(boolean) // optional {@link TimeTable#blinkTable() blinkTable}
   *    .id(Object) // optional {@link TimeTable#id() id}
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
    private static final long INIT_BIT_INTERVAL = 0x1L;
<<<<<<< HEAD
    private static final long OPT_BIT_CLOCK = 0x1L;
    private static final long OPT_BIT_START_TIME = 0x2L;
    private static final long OPT_BIT_BLINK_TABLE = 0x4L;
    private static final long OPT_BIT_ID = 0x8L;
=======
    private static final long OPT_BIT_BLINK_TABLE = 0x1L;
>>>>>>> main
    private long initBits = 0x1L;
    private long optBits;

    private Clock clock;
    private Duration interval;
    private Instant startTime;
    private boolean blinkTable;
    private Object id;

    private Builder() {
    }

    /**
<<<<<<< HEAD
=======
     * Fill a builder with attribute values from the provided {@code TimeTable} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(TimeTable instance) {
      Objects.requireNonNull(instance, "instance");
      clock(instance.clock());
      interval(instance.interval());
      Optional<Instant> startTimeOptional = instance.startTime();
      if (startTimeOptional.isPresent()) {
        startTime(startTimeOptional);
      }
      blinkTable(instance.blinkTable());
      id(instance.id());
      return this;
    }

    /**
>>>>>>> main
     * Initializes the value for the {@link TimeTable#clock() clock} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TimeTable#clock() clock}.</em>
     * @param clock The value for clock 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder clock(Clock clock) {
<<<<<<< HEAD
      checkNotIsSet(clockIsSet(), "clock");
      this.clock = Objects.requireNonNull(clock, "clock");
      optBits |= OPT_BIT_CLOCK;
=======
      this.clock = Objects.requireNonNull(clock, "clock");
>>>>>>> main
      return this;
    }

    /**
     * Initializes the value for the {@link TimeTable#interval() interval} attribute.
     * @param interval The value for interval 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder interval(Duration interval) {
<<<<<<< HEAD
      checkNotIsSet(intervalIsSet(), "interval");
=======
>>>>>>> main
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
<<<<<<< HEAD
      checkNotIsSet(startTimeIsSet(), "startTime");
      this.startTime = Objects.requireNonNull(startTime, "startTime");
      optBits |= OPT_BIT_START_TIME;
=======
      this.startTime = Objects.requireNonNull(startTime, "startTime");
>>>>>>> main
      return this;
    }

    /**
     * Initializes the optional value {@link TimeTable#startTime() startTime} to startTime.
     * @param startTime The value for startTime
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder startTime(Optional<? extends Instant> startTime) {
<<<<<<< HEAD
      checkNotIsSet(startTimeIsSet(), "startTime");
      this.startTime = startTime.orElse(null);
      optBits |= OPT_BIT_START_TIME;
=======
      this.startTime = startTime.orElse(null);
>>>>>>> main
      return this;
    }

    /**
     * Initializes the value for the {@link TimeTable#blinkTable() blinkTable} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TimeTable#blinkTable() blinkTable}.</em>
     * @param blinkTable The value for blinkTable 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder blinkTable(boolean blinkTable) {
<<<<<<< HEAD
      checkNotIsSet(blinkTableIsSet(), "blinkTable");
=======
>>>>>>> main
      this.blinkTable = blinkTable;
      optBits |= OPT_BIT_BLINK_TABLE;
      return this;
    }

    /**
     * Initializes the value for the {@link TimeTable#id() id} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TimeTable#id() id}.</em>
     * @param id The value for id 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder id(Object id) {
<<<<<<< HEAD
      checkNotIsSet(idIsSet(), "id");
      this.id = Objects.requireNonNull(id, "id");
      optBits |= OPT_BIT_ID;
=======
      this.id = Objects.requireNonNull(id, "id");
>>>>>>> main
      return this;
    }

    /**
     * Builds a new {@link ImmutableTimeTable ImmutableTimeTable}.
     * @return An immutable instance of TimeTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableTimeTable build() {
<<<<<<< HEAD
      checkRequiredAttributes();
      return ImmutableTimeTable.validate(new ImmutableTimeTable(this));
    }

    private boolean clockIsSet() {
      return (optBits & OPT_BIT_CLOCK) != 0;
    }

    private boolean startTimeIsSet() {
      return (optBits & OPT_BIT_START_TIME) != 0;
    }

=======
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableTimeTable.validate(new ImmutableTimeTable(this));
    }

>>>>>>> main
    private boolean blinkTableIsSet() {
      return (optBits & OPT_BIT_BLINK_TABLE) != 0;
    }

<<<<<<< HEAD
    private boolean idIsSet() {
      return (optBits & OPT_BIT_ID) != 0;
    }

    private boolean intervalIsSet() {
      return (initBits & INIT_BIT_INTERVAL) == 0;
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
      if (!intervalIsSet()) attributes.add("interval");
=======
    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_INTERVAL) != 0) attributes.add("interval");
>>>>>>> main
      return "Cannot build TimeTable, some of required attributes are not set " + attributes;
    }
  }
}
