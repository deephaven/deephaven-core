package io.deephaven.kafka.protobuf;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import com.google.protobuf.Message;
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
 * Immutable implementation of {@link DescriptorMessageClass}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableDescriptorMessageClass.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableDescriptorMessageClass.of()}.
 */
@Generated(from = "DescriptorMessageClass", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableDescriptorMessageClass<T extends Message>
    extends DescriptorMessageClass<T> {
  private final Class<T> clazz;

  private ImmutableDescriptorMessageClass(Class<T> clazz) {
    this.clazz = Objects.requireNonNull(clazz, "clazz");
  }

  private ImmutableDescriptorMessageClass(ImmutableDescriptorMessageClass<T> original, Class<T> clazz) {
    this.clazz = clazz;
  }

  /**
   * The message class.
   * @return the message class
   */
  @Override
  public Class<T> clazz() {
    return clazz;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DescriptorMessageClass#clazz() clazz} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for clazz
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDescriptorMessageClass<T> withClazz(Class<T> value) {
    if (this.clazz == value) return this;
    Class<T> newValue = Objects.requireNonNull(value, "clazz");
    return new ImmutableDescriptorMessageClass<>(this, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableDescriptorMessageClass} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableDescriptorMessageClass<?>
        && equalTo(0, (ImmutableDescriptorMessageClass<?>) another);
  }

  private boolean equalTo(int synthetic, ImmutableDescriptorMessageClass<?> another) {
    return clazz.equals(another.clazz);
  }

  /**
   * Computes a hash code from attributes: {@code clazz}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + clazz.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code DescriptorMessageClass} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("DescriptorMessageClass")
        .omitNullValues()
        .add("clazz", clazz)
        .toString();
  }

  /**
   * Construct a new immutable {@code DescriptorMessageClass} instance.
 * @param <T> generic parameter T
   * @param clazz The value for the {@code clazz} attribute
   * @return An immutable DescriptorMessageClass instance
   */
  public static <T extends Message> ImmutableDescriptorMessageClass<T> of(Class<T> clazz) {
    return new ImmutableDescriptorMessageClass<>(clazz);
  }

  /**
   * Creates an immutable copy of a {@link DescriptorMessageClass} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param <T> generic parameter T
   * @param instance The instance to copy
   * @return A copied immutable DescriptorMessageClass instance
   */
  public static <T extends Message> ImmutableDescriptorMessageClass<T> copyOf(DescriptorMessageClass<T> instance) {
    if (instance instanceof ImmutableDescriptorMessageClass<?>) {
      return (ImmutableDescriptorMessageClass<T>) instance;
    }
    return ImmutableDescriptorMessageClass.<T>builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableDescriptorMessageClass ImmutableDescriptorMessageClass}.
   * <pre>
   * ImmutableDescriptorMessageClass.&amp;lt;T&amp;gt;builder()
   *    .clazz(Class&amp;lt;T&amp;gt;) // required {@link DescriptorMessageClass#clazz() clazz}
   *    .build();
   * </pre>
   * @param <T> generic parameter T
   * @return A new ImmutableDescriptorMessageClass builder
   */
  public static <T extends Message> ImmutableDescriptorMessageClass.Builder<T> builder() {
    return new ImmutableDescriptorMessageClass.Builder<>();
  }

  /**
   * Builds instances of type {@link ImmutableDescriptorMessageClass ImmutableDescriptorMessageClass}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "DescriptorMessageClass", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder<T extends Message> {
    private static final long INIT_BIT_CLAZZ = 0x1L;
    private long initBits = 0x1L;

    private @Nullable Class<T> clazz;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code DescriptorMessageClass} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder<T> from(DescriptorMessageClass<T> instance) {
      Objects.requireNonNull(instance, "instance");
      clazz(instance.clazz());
      return this;
    }

    /**
     * Initializes the value for the {@link DescriptorMessageClass#clazz() clazz} attribute.
     * @param clazz The value for clazz 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder<T> clazz(Class<T> clazz) {
      this.clazz = Objects.requireNonNull(clazz, "clazz");
      initBits &= ~INIT_BIT_CLAZZ;
      return this;
    }

    /**
     * Builds a new {@link ImmutableDescriptorMessageClass ImmutableDescriptorMessageClass}.
     * @return An immutable instance of DescriptorMessageClass
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableDescriptorMessageClass<T> build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableDescriptorMessageClass<>(null, clazz);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_CLAZZ) != 0) attributes.add("clazz");
      return "Cannot build DescriptorMessageClass, some of required attributes are not set " + attributes;
    }
  }
}
