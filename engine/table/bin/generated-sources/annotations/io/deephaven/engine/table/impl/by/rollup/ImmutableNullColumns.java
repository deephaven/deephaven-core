package io.deephaven.engine.table.impl.by.rollup;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link NullColumns}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableNullColumns.builder()}.
 */
@Generated(from = "NullColumns", generator = "Immutables")
@SuppressWarnings({"all"})
@SuppressFBWarnings
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableNullColumns extends NullColumns {
  private final ImmutableMap<String, Class<?>> resultColumns;

  private ImmutableNullColumns(ImmutableMap<String, Class<?>> resultColumns) {
    this.resultColumns = resultColumns;
  }

  /**
   * @return The value of the {@code resultColumns} attribute
   */
  @Override
  public ImmutableMap<String, Class<?>> resultColumns() {
    return resultColumns;
  }

  /**
   * Copy the current immutable object by replacing the {@link NullColumns#resultColumns() resultColumns} map with the specified map.
   * Nulls are not permitted as keys or values.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param entries The entries to be added to the resultColumns map
   * @return A modified copy of {@code this} object
   */
  public final ImmutableNullColumns withResultColumns(Map<String, ? extends Class<?>> entries) {
    if (this.resultColumns == entries) return this;
    ImmutableMap<String, Class<?>> newValue = ImmutableMap.copyOf(entries);
    return validate(new ImmutableNullColumns(newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableNullColumns} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableNullColumns
        && equalTo(0, (ImmutableNullColumns) another);
  }

  private boolean equalTo(int synthetic, ImmutableNullColumns another) {
    return resultColumns.equals(another.resultColumns);
  }

  /**
   * Computes a hash code from attributes: {@code resultColumns}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + resultColumns.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code NullColumns} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("NullColumns")
        .omitNullValues()
        .add("resultColumns", resultColumns)
        .toString();
  }

  private static ImmutableNullColumns validate(ImmutableNullColumns instance) {
    instance.checkNonEmpty();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link NullColumns} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable NullColumns instance
   */
  public static ImmutableNullColumns copyOf(NullColumns instance) {
    if (instance instanceof ImmutableNullColumns) {
      return (ImmutableNullColumns) instance;
    }
    return ImmutableNullColumns.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableNullColumns ImmutableNullColumns}.
   * <pre>
   * ImmutableNullColumns.builder()
   *    .putResultColumns|putAllResultColumns(String =&gt; Class&amp;lt;?&amp;gt;) // {@link NullColumns#resultColumns() resultColumns} mappings
   *    .build();
   * </pre>
   * @return A new ImmutableNullColumns builder
   */
  public static ImmutableNullColumns.Builder builder() {
    return new ImmutableNullColumns.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableNullColumns ImmutableNullColumns}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "NullColumns", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements NullColumns.Builder {
    private ImmutableMap.Builder<String, Class<?>> resultColumns = ImmutableMap.builder();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code NullColumns} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(NullColumns instance) {
      Objects.requireNonNull(instance, "instance");
      putAllResultColumns(instance.resultColumns());
      return this;
    }

    /**
     * Put one entry to the {@link NullColumns#resultColumns() resultColumns} map.
     * @param key The key in the resultColumns map
     * @param value The associated value in the resultColumns map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder putResultColumns(String key, Class<?> value) {
      this.resultColumns.put(key, value);
      return this;
    }

    /**
     * Put one entry to the {@link NullColumns#resultColumns() resultColumns} map. Nulls are not permitted
     * @param entry The key and value entry
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder putResultColumns(Map.Entry<String, ? extends Class<?>> entry) {
      this.resultColumns.put(entry);
      return this;
    }

    /**
     * Sets or replaces all mappings from the specified map as entries for the {@link NullColumns#resultColumns() resultColumns} map. Nulls are not permitted
     * @param entries The entries that will be added to the resultColumns map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder resultColumns(Map<String, ? extends Class<?>> entries) {
      this.resultColumns = ImmutableMap.builder();
      return putAllResultColumns(entries);
    }

    /**
     * Put all mappings from the specified map as entries to {@link NullColumns#resultColumns() resultColumns} map. Nulls are not permitted
     * @param entries The entries that will be added to the resultColumns map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder putAllResultColumns(Map<String, ? extends Class<?>> entries) {
      this.resultColumns.putAll(entries);
      return this;
    }

    /**
     * Builds a new {@link ImmutableNullColumns ImmutableNullColumns}.
     * @return An immutable instance of NullColumns
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableNullColumns build() {
      return ImmutableNullColumns.validate(new ImmutableNullColumns(resultColumns.build()));
    }
  }
}
