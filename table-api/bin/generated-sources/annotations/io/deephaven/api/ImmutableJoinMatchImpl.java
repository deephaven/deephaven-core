package io.deephaven.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link JoinMatchImpl}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableJoinMatchImpl.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableJoinMatchImpl.of()}.
 */
@Generated(from = "JoinMatchImpl", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
final class ImmutableJoinMatchImpl extends JoinMatchImpl {
  private final ColumnName left;
  private final ColumnName right;

  private ImmutableJoinMatchImpl(ColumnName left, ColumnName right) {
    this.left = Objects.requireNonNull(left, "left");
    this.right = Objects.requireNonNull(right, "right");
  }

  private ImmutableJoinMatchImpl(
      ImmutableJoinMatchImpl original,
      ColumnName left,
      ColumnName right) {
    this.left = left;
    this.right = right;
  }

  /**
   * @return The value of the {@code left} attribute
   */
  @Override
  public ColumnName left() {
    return left;
  }

  /**
   * @return The value of the {@code right} attribute
   */
  @Override
  public ColumnName right() {
    return right;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JoinMatchImpl#left() left} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for left
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJoinMatchImpl withLeft(ColumnName value) {
    if (this.left == value) return this;
    ColumnName newValue = Objects.requireNonNull(value, "left");
    return validate(new ImmutableJoinMatchImpl(this, newValue, this.right));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JoinMatchImpl#right() right} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for right
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJoinMatchImpl withRight(ColumnName value) {
    if (this.right == value) return this;
    ColumnName newValue = Objects.requireNonNull(value, "right");
    return validate(new ImmutableJoinMatchImpl(this, this.left, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableJoinMatchImpl} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableJoinMatchImpl
        && equalTo(0, (ImmutableJoinMatchImpl) another);
  }

  private boolean equalTo(int synthetic, ImmutableJoinMatchImpl another) {
    return left.equals(another.left)
        && right.equals(another.right);
  }

  /**
   * Computes a hash code from attributes: {@code left}, {@code right}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + left.hashCode();
    h += (h << 5) + right.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code JoinMatchImpl} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "JoinMatchImpl{"
        + "left=" + left
        + ", right=" + right
        + "}";
  }

  /**
   * Construct a new immutable {@code JoinMatchImpl} instance.
   * @param left The value for the {@code left} attribute
   * @param right The value for the {@code right} attribute
   * @return An immutable JoinMatchImpl instance
   */
  public static ImmutableJoinMatchImpl of(ColumnName left, ColumnName right) {
    return validate(new ImmutableJoinMatchImpl(left, right));
  }

  private static ImmutableJoinMatchImpl validate(ImmutableJoinMatchImpl instance) {
    instance.checkNotSameColumn();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link JoinMatchImpl} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable JoinMatchImpl instance
   */
  public static ImmutableJoinMatchImpl copyOf(JoinMatchImpl instance) {
    if (instance instanceof ImmutableJoinMatchImpl) {
      return (ImmutableJoinMatchImpl) instance;
    }
    return ImmutableJoinMatchImpl.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableJoinMatchImpl ImmutableJoinMatchImpl}.
   * <pre>
   * ImmutableJoinMatchImpl.builder()
   *    .left(io.deephaven.api.ColumnName) // required {@link JoinMatchImpl#left() left}
   *    .right(io.deephaven.api.ColumnName) // required {@link JoinMatchImpl#right() right}
   *    .build();
   * </pre>
   * @return A new ImmutableJoinMatchImpl builder
   */
  public static ImmutableJoinMatchImpl.Builder builder() {
    return new ImmutableJoinMatchImpl.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableJoinMatchImpl ImmutableJoinMatchImpl}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "JoinMatchImpl", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_LEFT = 0x1L;
    private static final long INIT_BIT_RIGHT = 0x2L;
    private long initBits = 0x3L;

    private @Nullable ColumnName left;
    private @Nullable ColumnName right;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.api.JoinMatch} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(JoinMatch instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.api.JoinMatchImpl} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(JoinMatchImpl instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    private void from(Object object) {
      if (object instanceof JoinMatch) {
        JoinMatch instance = (JoinMatch) object;
        left(instance.left());
        right(instance.right());
      }
    }

    /**
     * Initializes the value for the {@link JoinMatchImpl#left() left} attribute.
     * @param left The value for left 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder left(ColumnName left) {
      this.left = Objects.requireNonNull(left, "left");
      initBits &= ~INIT_BIT_LEFT;
      return this;
    }

    /**
     * Initializes the value for the {@link JoinMatchImpl#right() right} attribute.
     * @param right The value for right 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder right(ColumnName right) {
      this.right = Objects.requireNonNull(right, "right");
      initBits &= ~INIT_BIT_RIGHT;
      return this;
    }

    /**
     * Builds a new {@link ImmutableJoinMatchImpl ImmutableJoinMatchImpl}.
     * @return An immutable instance of JoinMatchImpl
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableJoinMatchImpl build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableJoinMatchImpl.validate(new ImmutableJoinMatchImpl(null, left, right));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_LEFT) != 0) attributes.add("left");
      if ((initBits & INIT_BIT_RIGHT) != 0) attributes.add("right");
      return "Cannot build JoinMatchImpl, some of required attributes are not set " + attributes;
    }
  }
}
