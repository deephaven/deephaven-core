package io.deephaven.qst.type;

import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ByteType}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableByteType.builder()}.
 */
@Generated(from = "ByteType", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableByteType extends ByteType {

  private ImmutableByteType(ImmutableByteType.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableByteType} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableByteType
        && equalTo(0, (ImmutableByteType) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableByteType another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return -1396748451;
  }

  /**
   * Creates an immutable copy of a {@link ByteType} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ByteType instance
   */
  public static ImmutableByteType copyOf(ByteType instance) {
    if (instance instanceof ImmutableByteType) {
      return (ImmutableByteType) instance;
    }
    return ImmutableByteType.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableByteType ImmutableByteType}.
   * <pre>
   * ImmutableByteType.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableByteType builder
   */
  public static ImmutableByteType.Builder builder() {
    return new ImmutableByteType.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableByteType ImmutableByteType}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ByteType", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ByteType} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(ByteType instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableByteType ImmutableByteType}.
     * @return An immutable instance of ByteType
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableByteType build() {
      return new ImmutableByteType(this);
    }
  }
}
