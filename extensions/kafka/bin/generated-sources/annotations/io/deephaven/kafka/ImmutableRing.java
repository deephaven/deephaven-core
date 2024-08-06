package io.deephaven.kafka;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link KafkaTools.TableType.Ring}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableRing.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableRing.of()}.
 */
@Generated(from = "KafkaTools.TableType.Ring", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableRing extends KafkaTools.TableType.Ring {
  private final int capacity;

  private ImmutableRing(int capacity) {
    this.capacity = capacity;
  }

  /**
   * @return The value of the {@code capacity} attribute
   */
  @Override
  public int capacity() {
    return capacity;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link KafkaTools.TableType.Ring#capacity() capacity} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for capacity
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRing withCapacity(int value) {
    if (this.capacity == value) return this;
    return new ImmutableRing(value);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableRing} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableRing
        && equalTo(0, (ImmutableRing) another);
  }

  private boolean equalTo(int synthetic, ImmutableRing another) {
    return capacity == another.capacity;
  }

  /**
   * Computes a hash code from attributes: {@code capacity}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + capacity;
    return h;
  }

  /**
   * Prints the immutable value {@code Ring} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("Ring")
        .omitNullValues()
        .add("capacity", capacity)
        .toString();
  }

  /**
   * Construct a new immutable {@code Ring} instance.
   * @param capacity The value for the {@code capacity} attribute
   * @return An immutable Ring instance
   */
  public static ImmutableRing of(int capacity) {
    return new ImmutableRing(capacity);
  }

  /**
   * Creates an immutable copy of a {@link KafkaTools.TableType.Ring} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable Ring instance
   */
  public static ImmutableRing copyOf(KafkaTools.TableType.Ring instance) {
    if (instance instanceof ImmutableRing) {
      return (ImmutableRing) instance;
    }
    return ImmutableRing.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableRing ImmutableRing}.
   * <pre>
   * ImmutableRing.builder()
   *    .capacity(int) // required {@link KafkaTools.TableType.Ring#capacity() capacity}
   *    .build();
   * </pre>
   * @return A new ImmutableRing builder
   */
  public static ImmutableRing.Builder builder() {
    return new ImmutableRing.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableRing ImmutableRing}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "KafkaTools.TableType.Ring", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_CAPACITY = 0x1L;
    private long initBits = 0x1L;

    private int capacity;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code Ring} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(KafkaTools.TableType.Ring instance) {
      Objects.requireNonNull(instance, "instance");
      capacity(instance.capacity());
      return this;
    }

    /**
     * Initializes the value for the {@link KafkaTools.TableType.Ring#capacity() capacity} attribute.
     * @param capacity The value for capacity 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder capacity(int capacity) {
      this.capacity = capacity;
      initBits &= ~INIT_BIT_CAPACITY;
      return this;
    }

    /**
     * Builds a new {@link ImmutableRing ImmutableRing}.
     * @return An immutable instance of Ring
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableRing build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableRing(capacity);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_CAPACITY) != 0) attributes.add("capacity");
      return "Cannot build Ring, some of required attributes are not set " + attributes;
    }
  }
}
