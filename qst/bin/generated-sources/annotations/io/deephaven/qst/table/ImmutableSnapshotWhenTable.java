package io.deephaven.qst.table;

import io.deephaven.api.snapshot.SnapshotWhenOptions;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link SnapshotWhenTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableSnapshotWhenTable.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableSnapshotWhenTable.of()}.
 */
@Generated(from = "SnapshotWhenTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableSnapshotWhenTable extends SnapshotWhenTable {
  private transient final int depth;
  private final TableSpec base;
  private final TableSpec trigger;
  private final SnapshotWhenOptions options;
  private transient final int hashCode;

  private ImmutableSnapshotWhenTable(
      TableSpec base,
      TableSpec trigger,
      SnapshotWhenOptions options) {
    this.base = Objects.requireNonNull(base, "base");
    this.trigger = Objects.requireNonNull(trigger, "trigger");
    this.options = Objects.requireNonNull(options, "options");
    this.depth = super.depth();
    this.hashCode = computeHashCode();
  }

  private ImmutableSnapshotWhenTable(
      ImmutableSnapshotWhenTable original,
      TableSpec base,
      TableSpec trigger,
      SnapshotWhenOptions options) {
    this.base = base;
    this.trigger = trigger;
    this.options = options;
    this.depth = super.depth();
    this.hashCode = computeHashCode();
  }

  /**
   * @return The computed-at-construction value of the {@code depth} attribute
   */
  @Override
  public int depth() {
    return depth;
  }

  /**
   * @return The value of the {@code base} attribute
   */
  @Override
  public TableSpec base() {
    return base;
  }

  /**
   * @return The value of the {@code trigger} attribute
   */
  @Override
  public TableSpec trigger() {
    return trigger;
  }

  /**
   * @return The value of the {@code options} attribute
   */
  @Override
  public SnapshotWhenOptions options() {
    return options;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SnapshotWhenTable#base() base} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for base
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSnapshotWhenTable withBase(TableSpec value) {
    if (this.base == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "base");
    return validate(new ImmutableSnapshotWhenTable(this, newValue, this.trigger, this.options));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SnapshotWhenTable#trigger() trigger} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for trigger
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSnapshotWhenTable withTrigger(TableSpec value) {
    if (this.trigger == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "trigger");
    return validate(new ImmutableSnapshotWhenTable(this, this.base, newValue, this.options));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SnapshotWhenTable#options() options} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for options
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSnapshotWhenTable withOptions(SnapshotWhenOptions value) {
    if (this.options == value) return this;
    SnapshotWhenOptions newValue = Objects.requireNonNull(value, "options");
    return validate(new ImmutableSnapshotWhenTable(this, this.base, this.trigger, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableSnapshotWhenTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableSnapshotWhenTable
        && equalTo(0, (ImmutableSnapshotWhenTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableSnapshotWhenTable another) {
    if (hashCode != another.hashCode) return false;
    return depth == another.depth
        && base.equals(another.base)
        && trigger.equals(another.trigger)
        && options.equals(another.options);
  }

  /**
   * Returns a precomputed-on-construction hash code from attributes: {@code depth}, {@code base}, {@code trigger}, {@code options}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return hashCode;
  }

  private int computeHashCode() {
    int h = 5381;
    h += (h << 5) + getClass().hashCode();
    h += (h << 5) + depth;
    h += (h << 5) + base.hashCode();
    h += (h << 5) + trigger.hashCode();
    h += (h << 5) + options.hashCode();
    return h;
  }

  private static final class InternerHolder {
    static final Map<ImmutableSnapshotWhenTable, WeakReference<ImmutableSnapshotWhenTable>> INTERNER =
        new WeakHashMap<>();
  }

  /**
   * Construct a new immutable {@code SnapshotWhenTable} instance.
   * @param base The value for the {@code base} attribute
   * @param trigger The value for the {@code trigger} attribute
   * @param options The value for the {@code options} attribute
   * @return An immutable SnapshotWhenTable instance
   */
  public static ImmutableSnapshotWhenTable of(TableSpec base, TableSpec trigger, SnapshotWhenOptions options) {
    return validate(new ImmutableSnapshotWhenTable(base, trigger, options));
  }

  private static ImmutableSnapshotWhenTable validate(ImmutableSnapshotWhenTable instance) {
    synchronized (InternerHolder.INTERNER) {
      WeakReference<ImmutableSnapshotWhenTable> reference = InternerHolder.INTERNER.get(instance);
      ImmutableSnapshotWhenTable interned = reference != null ? reference.get() : null;
      if (interned == null) {
        InternerHolder.INTERNER.put(instance, new WeakReference<>(instance));
        interned = instance;
      }
      return interned;
    }
  }

  /**
   * Creates an immutable copy of a {@link SnapshotWhenTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable SnapshotWhenTable instance
   */
  public static ImmutableSnapshotWhenTable copyOf(SnapshotWhenTable instance) {
    if (instance instanceof ImmutableSnapshotWhenTable) {
      return (ImmutableSnapshotWhenTable) instance;
    }
    return ImmutableSnapshotWhenTable.builder()
        .base(instance.base())
        .trigger(instance.trigger())
        .options(instance.options())
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableSnapshotWhenTable ImmutableSnapshotWhenTable}.
   * <pre>
   * ImmutableSnapshotWhenTable.builder()
   *    .base(io.deephaven.qst.table.TableSpec) // required {@link SnapshotWhenTable#base() base}
   *    .trigger(io.deephaven.qst.table.TableSpec) // required {@link SnapshotWhenTable#trigger() trigger}
   *    .options(io.deephaven.api.snapshot.SnapshotWhenOptions) // required {@link SnapshotWhenTable#options() options}
   *    .build();
   * </pre>
   * @return A new ImmutableSnapshotWhenTable builder
   */
  public static ImmutableSnapshotWhenTable.Builder builder() {
    return new ImmutableSnapshotWhenTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableSnapshotWhenTable ImmutableSnapshotWhenTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "SnapshotWhenTable", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_BASE = 0x1L;
    private static final long INIT_BIT_TRIGGER = 0x2L;
    private static final long INIT_BIT_OPTIONS = 0x4L;
    private long initBits = 0x7L;

    private TableSpec base;
    private TableSpec trigger;
    private SnapshotWhenOptions options;

    private Builder() {
    }

    /**
     * Initializes the value for the {@link SnapshotWhenTable#base() base} attribute.
     * @param base The value for base 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder base(TableSpec base) {
      checkNotIsSet(baseIsSet(), "base");
      this.base = Objects.requireNonNull(base, "base");
      initBits &= ~INIT_BIT_BASE;
      return this;
    }

    /**
     * Initializes the value for the {@link SnapshotWhenTable#trigger() trigger} attribute.
     * @param trigger The value for trigger 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder trigger(TableSpec trigger) {
      checkNotIsSet(triggerIsSet(), "trigger");
      this.trigger = Objects.requireNonNull(trigger, "trigger");
      initBits &= ~INIT_BIT_TRIGGER;
      return this;
    }

    /**
     * Initializes the value for the {@link SnapshotWhenTable#options() options} attribute.
     * @param options The value for options 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder options(SnapshotWhenOptions options) {
      checkNotIsSet(optionsIsSet(), "options");
      this.options = Objects.requireNonNull(options, "options");
      initBits &= ~INIT_BIT_OPTIONS;
      return this;
    }

    /**
     * Builds a new {@link ImmutableSnapshotWhenTable ImmutableSnapshotWhenTable}.
     * @return An immutable instance of SnapshotWhenTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableSnapshotWhenTable build() {
      checkRequiredAttributes();
      return ImmutableSnapshotWhenTable.validate(new ImmutableSnapshotWhenTable(null, base, trigger, options));
    }

    private boolean baseIsSet() {
      return (initBits & INIT_BIT_BASE) == 0;
    }

    private boolean triggerIsSet() {
      return (initBits & INIT_BIT_TRIGGER) == 0;
    }

    private boolean optionsIsSet() {
      return (initBits & INIT_BIT_OPTIONS) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of SnapshotWhenTable is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!baseIsSet()) attributes.add("base");
      if (!triggerIsSet()) attributes.add("trigger");
      if (!optionsIsSet()) attributes.add("options");
      return "Cannot build SnapshotWhenTable, some of required attributes are not set " + attributes;
    }
  }
}
