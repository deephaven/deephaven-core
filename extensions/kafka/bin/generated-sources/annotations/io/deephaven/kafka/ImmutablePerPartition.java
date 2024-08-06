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
 * Immutable implementation of {@link KafkaTools.StreamConsumerRegistrarProvider.PerPartition}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutablePerPartition.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutablePerPartition.of()}.
 */
@Generated(from = "KafkaTools.StreamConsumerRegistrarProvider.PerPartition", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutablePerPartition
    extends KafkaTools.StreamConsumerRegistrarProvider.PerPartition {
  private final KafkaTools.PerPartitionConsumerRegistrar registrar;

  private ImmutablePerPartition(KafkaTools.PerPartitionConsumerRegistrar registrar) {
    this.registrar = Objects.requireNonNull(registrar, "registrar");
  }

  private ImmutablePerPartition(
      ImmutablePerPartition original,
      KafkaTools.PerPartitionConsumerRegistrar registrar) {
    this.registrar = registrar;
  }

  /**
   * @return The value of the {@code registrar} attribute
   */
  @Override
  public KafkaTools.PerPartitionConsumerRegistrar registrar() {
    return registrar;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link KafkaTools.StreamConsumerRegistrarProvider.PerPartition#registrar() registrar} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for registrar
   * @return A modified copy of the {@code this} object
   */
  public final ImmutablePerPartition withRegistrar(KafkaTools.PerPartitionConsumerRegistrar value) {
    if (this.registrar == value) return this;
    KafkaTools.PerPartitionConsumerRegistrar newValue = Objects.requireNonNull(value, "registrar");
    return new ImmutablePerPartition(this, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutablePerPartition} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutablePerPartition
        && equalTo(0, (ImmutablePerPartition) another);
  }

  private boolean equalTo(int synthetic, ImmutablePerPartition another) {
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
   * Prints the immutable value {@code PerPartition} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("PerPartition")
        .omitNullValues()
        .add("registrar", registrar)
        .toString();
  }

  /**
   * Construct a new immutable {@code PerPartition} instance.
   * @param registrar The value for the {@code registrar} attribute
   * @return An immutable PerPartition instance
   */
  public static ImmutablePerPartition of(KafkaTools.PerPartitionConsumerRegistrar registrar) {
    return new ImmutablePerPartition(registrar);
  }

  /**
   * Creates an immutable copy of a {@link KafkaTools.StreamConsumerRegistrarProvider.PerPartition} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable PerPartition instance
   */
  public static ImmutablePerPartition copyOf(KafkaTools.StreamConsumerRegistrarProvider.PerPartition instance) {
    if (instance instanceof ImmutablePerPartition) {
      return (ImmutablePerPartition) instance;
    }
    return ImmutablePerPartition.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutablePerPartition ImmutablePerPartition}.
   * <pre>
   * ImmutablePerPartition.builder()
   *    .registrar(io.deephaven.kafka.KafkaTools.PerPartitionConsumerRegistrar) // required {@link KafkaTools.StreamConsumerRegistrarProvider.PerPartition#registrar() registrar}
   *    .build();
   * </pre>
   * @return A new ImmutablePerPartition builder
   */
  public static ImmutablePerPartition.Builder builder() {
    return new ImmutablePerPartition.Builder();
  }

  /**
   * Builds instances of type {@link ImmutablePerPartition ImmutablePerPartition}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "KafkaTools.StreamConsumerRegistrarProvider.PerPartition", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_REGISTRAR = 0x1L;
    private long initBits = 0x1L;

    private @Nullable KafkaTools.PerPartitionConsumerRegistrar registrar;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code PerPartition} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(KafkaTools.StreamConsumerRegistrarProvider.PerPartition instance) {
      Objects.requireNonNull(instance, "instance");
      registrar(instance.registrar());
      return this;
    }

    /**
     * Initializes the value for the {@link KafkaTools.StreamConsumerRegistrarProvider.PerPartition#registrar() registrar} attribute.
     * @param registrar The value for registrar 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder registrar(KafkaTools.PerPartitionConsumerRegistrar registrar) {
      this.registrar = Objects.requireNonNull(registrar, "registrar");
      initBits &= ~INIT_BIT_REGISTRAR;
      return this;
    }

    /**
     * Builds a new {@link ImmutablePerPartition ImmutablePerPartition}.
     * @return An immutable instance of PerPartition
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutablePerPartition build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutablePerPartition(null, registrar);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_REGISTRAR) != 0) attributes.add("registrar");
      return "Cannot build PerPartition, some of required attributes are not set " + attributes;
    }
  }
}
