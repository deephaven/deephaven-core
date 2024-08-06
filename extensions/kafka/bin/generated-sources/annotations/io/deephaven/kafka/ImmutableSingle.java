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
 * Immutable implementation of {@link KafkaTools.StreamConsumerRegistrarProvider.Single}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableSingle.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableSingle.of()}.
 */
@Generated(from = "KafkaTools.StreamConsumerRegistrarProvider.Single", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableSingle
    extends KafkaTools.StreamConsumerRegistrarProvider.Single {
  private final KafkaTools.SingleConsumerRegistrar registrar;

  private ImmutableSingle(KafkaTools.SingleConsumerRegistrar registrar) {
    this.registrar = Objects.requireNonNull(registrar, "registrar");
  }

  private ImmutableSingle(ImmutableSingle original, KafkaTools.SingleConsumerRegistrar registrar) {
    this.registrar = registrar;
  }

  /**
   * @return The value of the {@code registrar} attribute
   */
  @Override
  public KafkaTools.SingleConsumerRegistrar registrar() {
    return registrar;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link KafkaTools.StreamConsumerRegistrarProvider.Single#registrar() registrar} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for registrar
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSingle withRegistrar(KafkaTools.SingleConsumerRegistrar value) {
    if (this.registrar == value) return this;
    KafkaTools.SingleConsumerRegistrar newValue = Objects.requireNonNull(value, "registrar");
    return new ImmutableSingle(this, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableSingle} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableSingle
        && equalTo(0, (ImmutableSingle) another);
  }

  private boolean equalTo(int synthetic, ImmutableSingle another) {
    return registrar.equals(another.registrar);
  }

  /**
   * Computes a hash code from attributes: {@code registrar}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + registrar.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code Single} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("Single")
        .omitNullValues()
        .add("registrar", registrar)
        .toString();
  }

  /**
   * Construct a new immutable {@code Single} instance.
   * @param registrar The value for the {@code registrar} attribute
   * @return An immutable Single instance
   */
  public static ImmutableSingle of(KafkaTools.SingleConsumerRegistrar registrar) {
    return new ImmutableSingle(registrar);
  }

  /**
   * Creates an immutable copy of a {@link KafkaTools.StreamConsumerRegistrarProvider.Single} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable Single instance
   */
  public static ImmutableSingle copyOf(KafkaTools.StreamConsumerRegistrarProvider.Single instance) {
    if (instance instanceof ImmutableSingle) {
      return (ImmutableSingle) instance;
    }
    return ImmutableSingle.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableSingle ImmutableSingle}.
   * <pre>
   * ImmutableSingle.builder()
   *    .registrar(io.deephaven.kafka.KafkaTools.SingleConsumerRegistrar) // required {@link KafkaTools.StreamConsumerRegistrarProvider.Single#registrar() registrar}
   *    .build();
   * </pre>
   * @return A new ImmutableSingle builder
   */
  public static ImmutableSingle.Builder builder() {
    return new ImmutableSingle.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableSingle ImmutableSingle}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "KafkaTools.StreamConsumerRegistrarProvider.Single", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_REGISTRAR = 0x1L;
    private long initBits = 0x1L;

    private @Nullable KafkaTools.SingleConsumerRegistrar registrar;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code Single} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(KafkaTools.StreamConsumerRegistrarProvider.Single instance) {
      Objects.requireNonNull(instance, "instance");
      registrar(instance.registrar());
      return this;
    }

    /**
     * Initializes the value for the {@link KafkaTools.StreamConsumerRegistrarProvider.Single#registrar() registrar} attribute.
     * @param registrar The value for registrar 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder registrar(KafkaTools.SingleConsumerRegistrar registrar) {
      this.registrar = Objects.requireNonNull(registrar, "registrar");
      initBits &= ~INIT_BIT_REGISTRAR;
      return this;
    }

    /**
     * Builds a new {@link ImmutableSingle ImmutableSingle}.
     * @return An immutable instance of Single
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableSingle build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableSingle(null, registrar);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_REGISTRAR) != 0) attributes.add("registrar");
      return "Cannot build Single, some of required attributes are not set " + attributes;
    }
  }
}
