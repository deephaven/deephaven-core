package io.deephaven.engine.table;

import com.google.common.base.MoreObjects;
import com.google.common.primitives.Booleans;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import io.deephaven.engine.rowset.RowSet;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link DataIndexTransformer}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableDataIndexTransformer.builder()}.
 */
@Generated(from = "DataIndexTransformer", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableDataIndexTransformer implements DataIndexTransformer {
  private final @Nullable RowSet intersectRowSet;
  private final @Nullable RowSet invertRowSet;
  private final boolean sortByFirstRowKey;

  private ImmutableDataIndexTransformer(ImmutableDataIndexTransformer.Builder builder) {
    this.intersectRowSet = builder.intersectRowSet;
    this.invertRowSet = builder.invertRowSet;
    this.sortByFirstRowKey = builder.sortByFirstRowKeyIsSet()
        ? builder.sortByFirstRowKey
        : DataIndexTransformer.super.sortByFirstRowKey();
  }

  private ImmutableDataIndexTransformer(
      @Nullable RowSet intersectRowSet,
      @Nullable RowSet invertRowSet,
      boolean sortByFirstRowKey) {
    this.intersectRowSet = intersectRowSet;
    this.invertRowSet = invertRowSet;
    this.sortByFirstRowKey = sortByFirstRowKey;
  }

  /**
   * A {@link RowSet} to {@link RowSet#intersect(RowSet) intersect} with input RowSets when producing output RowSets.
   * If present, the result {@link BasicDataIndex} will be a static snapshot. This is the first transformation applied
   * if present.
   */
  @Override
  public Optional<RowSet> intersectRowSet() {
    return Optional.ofNullable(intersectRowSet);
  }

  /**
   * @return The value of the {@code invertRowSet} attribute
   */
  @Override
  public Optional<RowSet> invertRowSet() {
    return Optional.ofNullable(invertRowSet);
  }

  /**
   * Whether to sort the output {@link BasicDataIndex BasicDataIndex's} {@link BasicDataIndex#table() table} by the
   * first row key in each output {@link RowSet}. This is always applied after {@link #intersectRowSet()} and
   * {@link #invertRowSet()} if present. Note that when sorting a {@link BasicDataIndex#isRefreshing() refreshing}
   * index, operations that rely on the transformed index must be sure to depend on the <em>transformed</em> index,
   * and not the input index, for correct satisfaction.
   */
  @Override
  public boolean sortByFirstRowKey() {
    return sortByFirstRowKey;
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link DataIndexTransformer#intersectRowSet() intersectRowSet} attribute.
   * @param value The value for intersectRowSet
   * @return A modified copy of {@code this} object
   */
  public final ImmutableDataIndexTransformer withIntersectRowSet(RowSet value) {
    @Nullable RowSet newValue = Objects.requireNonNull(value, "intersectRowSet");
    if (this.intersectRowSet == newValue) return this;
    return validate(new ImmutableDataIndexTransformer(newValue, this.invertRowSet, this.sortByFirstRowKey));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link DataIndexTransformer#intersectRowSet() intersectRowSet} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for intersectRowSet
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableDataIndexTransformer withIntersectRowSet(Optional<? extends RowSet> optional) {
    @Nullable RowSet value = optional.orElse(null);
    if (this.intersectRowSet == value) return this;
    return validate(new ImmutableDataIndexTransformer(value, this.invertRowSet, this.sortByFirstRowKey));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link DataIndexTransformer#invertRowSet() invertRowSet} attribute.
   * @param value The value for invertRowSet
   * @return A modified copy of {@code this} object
   */
  public final ImmutableDataIndexTransformer withInvertRowSet(RowSet value) {
    @Nullable RowSet newValue = Objects.requireNonNull(value, "invertRowSet");
    if (this.invertRowSet == newValue) return this;
    return validate(new ImmutableDataIndexTransformer(this.intersectRowSet, newValue, this.sortByFirstRowKey));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link DataIndexTransformer#invertRowSet() invertRowSet} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for invertRowSet
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableDataIndexTransformer withInvertRowSet(Optional<? extends RowSet> optional) {
    @Nullable RowSet value = optional.orElse(null);
    if (this.invertRowSet == value) return this;
    return validate(new ImmutableDataIndexTransformer(this.intersectRowSet, value, this.sortByFirstRowKey));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DataIndexTransformer#sortByFirstRowKey() sortByFirstRowKey} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for sortByFirstRowKey
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDataIndexTransformer withSortByFirstRowKey(boolean value) {
    if (this.sortByFirstRowKey == value) return this;
    return validate(new ImmutableDataIndexTransformer(this.intersectRowSet, this.invertRowSet, value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableDataIndexTransformer} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableDataIndexTransformer
        && equalTo(0, (ImmutableDataIndexTransformer) another);
  }

  private boolean equalTo(int synthetic, ImmutableDataIndexTransformer another) {
    return Objects.equals(intersectRowSet, another.intersectRowSet)
        && Objects.equals(invertRowSet, another.invertRowSet)
        && sortByFirstRowKey == another.sortByFirstRowKey;
  }

  /**
   * Computes a hash code from attributes: {@code intersectRowSet}, {@code invertRowSet}, {@code sortByFirstRowKey}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + Objects.hashCode(intersectRowSet);
    h += (h << 5) + Objects.hashCode(invertRowSet);
    h += (h << 5) + Booleans.hashCode(sortByFirstRowKey);
    return h;
  }

  /**
   * Prints the immutable value {@code DataIndexTransformer} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("DataIndexTransformer")
        .omitNullValues()
        .add("intersectRowSet", intersectRowSet)
        .add("invertRowSet", invertRowSet)
        .add("sortByFirstRowKey", sortByFirstRowKey)
        .toString();
  }

  private static ImmutableDataIndexTransformer validate(ImmutableDataIndexTransformer instance) {
    instance.checkNotEmpty();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link DataIndexTransformer} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable DataIndexTransformer instance
   */
  public static ImmutableDataIndexTransformer copyOf(DataIndexTransformer instance) {
    if (instance instanceof ImmutableDataIndexTransformer) {
      return (ImmutableDataIndexTransformer) instance;
    }
    return ImmutableDataIndexTransformer.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableDataIndexTransformer ImmutableDataIndexTransformer}.
   * <pre>
   * ImmutableDataIndexTransformer.builder()
   *    .intersectRowSet(io.deephaven.engine.rowset.RowSet) // optional {@link DataIndexTransformer#intersectRowSet() intersectRowSet}
   *    .invertRowSet(io.deephaven.engine.rowset.RowSet) // optional {@link DataIndexTransformer#invertRowSet() invertRowSet}
   *    .sortByFirstRowKey(boolean) // optional {@link DataIndexTransformer#sortByFirstRowKey() sortByFirstRowKey}
   *    .build();
   * </pre>
   * @return A new ImmutableDataIndexTransformer builder
   */
  public static ImmutableDataIndexTransformer.Builder builder() {
    return new ImmutableDataIndexTransformer.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableDataIndexTransformer ImmutableDataIndexTransformer}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "DataIndexTransformer", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements DataIndexTransformer.Builder {
    private static final long OPT_BIT_SORT_BY_FIRST_ROW_KEY = 0x1L;
    private long optBits;

    private @Nullable RowSet intersectRowSet;
    private @Nullable RowSet invertRowSet;
    private boolean sortByFirstRowKey;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code DataIndexTransformer} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(DataIndexTransformer instance) {
      Objects.requireNonNull(instance, "instance");
      Optional<RowSet> intersectRowSetOptional = instance.intersectRowSet();
      if (intersectRowSetOptional.isPresent()) {
        intersectRowSet(intersectRowSetOptional);
      }
      Optional<RowSet> invertRowSetOptional = instance.invertRowSet();
      if (invertRowSetOptional.isPresent()) {
        invertRowSet(invertRowSetOptional);
      }
      sortByFirstRowKey(instance.sortByFirstRowKey());
      return this;
    }

    /**
     * Initializes the optional value {@link DataIndexTransformer#intersectRowSet() intersectRowSet} to intersectRowSet.
     * @param intersectRowSet The value for intersectRowSet
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder intersectRowSet(RowSet intersectRowSet) {
      this.intersectRowSet = Objects.requireNonNull(intersectRowSet, "intersectRowSet");
      return this;
    }

    /**
     * Initializes the optional value {@link DataIndexTransformer#intersectRowSet() intersectRowSet} to intersectRowSet.
     * @param intersectRowSet The value for intersectRowSet
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder intersectRowSet(Optional<? extends RowSet> intersectRowSet) {
      this.intersectRowSet = intersectRowSet.orElse(null);
      return this;
    }

    /**
     * Initializes the optional value {@link DataIndexTransformer#invertRowSet() invertRowSet} to invertRowSet.
     * @param invertRowSet The value for invertRowSet
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder invertRowSet(RowSet invertRowSet) {
      this.invertRowSet = Objects.requireNonNull(invertRowSet, "invertRowSet");
      return this;
    }

    /**
     * Initializes the optional value {@link DataIndexTransformer#invertRowSet() invertRowSet} to invertRowSet.
     * @param invertRowSet The value for invertRowSet
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder invertRowSet(Optional<? extends RowSet> invertRowSet) {
      this.invertRowSet = invertRowSet.orElse(null);
      return this;
    }

    /**
     * Initializes the value for the {@link DataIndexTransformer#sortByFirstRowKey() sortByFirstRowKey} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link DataIndexTransformer#sortByFirstRowKey() sortByFirstRowKey}.</em>
     * @param sortByFirstRowKey The value for sortByFirstRowKey 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder sortByFirstRowKey(boolean sortByFirstRowKey) {
      this.sortByFirstRowKey = sortByFirstRowKey;
      optBits |= OPT_BIT_SORT_BY_FIRST_ROW_KEY;
      return this;
    }

    /**
     * Builds a new {@link ImmutableDataIndexTransformer ImmutableDataIndexTransformer}.
     * @return An immutable instance of DataIndexTransformer
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableDataIndexTransformer build() {
      return ImmutableDataIndexTransformer.validate(new ImmutableDataIndexTransformer(this));
    }

    private boolean sortByFirstRowKeyIsSet() {
      return (optBits & OPT_BIT_SORT_BY_FIRST_ROW_KEY) != 0;
    }
  }
}
