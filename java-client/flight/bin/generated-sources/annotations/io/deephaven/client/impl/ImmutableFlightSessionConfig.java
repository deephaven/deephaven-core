package io.deephaven.client.impl;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.arrow.memory.BufferAllocator;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link FlightSessionConfig}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableFlightSessionConfig.builder()}.
 */
@Generated(from = "FlightSessionConfig", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableFlightSessionConfig extends FlightSessionConfig {
  private final @Nullable SessionConfig sessionConfig;
  private final @Nullable BufferAllocator allocator;

  private ImmutableFlightSessionConfig(
      @Nullable SessionConfig sessionConfig,
      @Nullable BufferAllocator allocator) {
    this.sessionConfig = sessionConfig;
    this.allocator = allocator;
  }

  /**
   * The session config.
   */
  @Override
  public Optional<SessionConfig> sessionConfig() {
    return Optional.ofNullable(sessionConfig);
  }

  /**
   * The allocator.
   */
  @Override
  public Optional<BufferAllocator> allocator() {
    return Optional.ofNullable(allocator);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link FlightSessionConfig#sessionConfig() sessionConfig} attribute.
   * @param value The value for sessionConfig
   * @return A modified copy of {@code this} object
   */
  public final ImmutableFlightSessionConfig withSessionConfig(SessionConfig value) {
    @Nullable SessionConfig newValue = Objects.requireNonNull(value, "sessionConfig");
    if (this.sessionConfig == newValue) return this;
    return new ImmutableFlightSessionConfig(newValue, this.allocator);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link FlightSessionConfig#sessionConfig() sessionConfig} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for sessionConfig
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableFlightSessionConfig withSessionConfig(Optional<? extends SessionConfig> optional) {
    @Nullable SessionConfig value = optional.orElse(null);
    if (this.sessionConfig == value) return this;
    return new ImmutableFlightSessionConfig(value, this.allocator);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link FlightSessionConfig#allocator() allocator} attribute.
   * @param value The value for allocator
   * @return A modified copy of {@code this} object
   */
  public final ImmutableFlightSessionConfig withAllocator(BufferAllocator value) {
    @Nullable BufferAllocator newValue = Objects.requireNonNull(value, "allocator");
    if (this.allocator == newValue) return this;
    return new ImmutableFlightSessionConfig(this.sessionConfig, newValue);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link FlightSessionConfig#allocator() allocator} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for allocator
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableFlightSessionConfig withAllocator(Optional<? extends BufferAllocator> optional) {
    @Nullable BufferAllocator value = optional.orElse(null);
    if (this.allocator == value) return this;
    return new ImmutableFlightSessionConfig(this.sessionConfig, value);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableFlightSessionConfig} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableFlightSessionConfig
        && equalTo(0, (ImmutableFlightSessionConfig) another);
  }

  private boolean equalTo(int synthetic, ImmutableFlightSessionConfig another) {
    return Objects.equals(sessionConfig, another.sessionConfig)
        && Objects.equals(allocator, another.allocator);
  }

  /**
   * Computes a hash code from attributes: {@code sessionConfig}, {@code allocator}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + Objects.hashCode(sessionConfig);
    h += (h << 5) + Objects.hashCode(allocator);
    return h;
  }

  /**
   * Prints the immutable value {@code FlightSessionConfig} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("FlightSessionConfig")
        .omitNullValues()
        .add("sessionConfig", sessionConfig)
        .add("allocator", allocator)
        .toString();
  }

  /**
   * Creates an immutable copy of a {@link FlightSessionConfig} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable FlightSessionConfig instance
   */
  public static ImmutableFlightSessionConfig copyOf(FlightSessionConfig instance) {
    if (instance instanceof ImmutableFlightSessionConfig) {
      return (ImmutableFlightSessionConfig) instance;
    }
    return ImmutableFlightSessionConfig.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableFlightSessionConfig ImmutableFlightSessionConfig}.
   * <pre>
   * ImmutableFlightSessionConfig.builder()
   *    .sessionConfig(io.deephaven.client.impl.SessionConfig) // optional {@link FlightSessionConfig#sessionConfig() sessionConfig}
   *    .allocator(org.apache.arrow.memory.BufferAllocator) // optional {@link FlightSessionConfig#allocator() allocator}
   *    .build();
   * </pre>
   * @return A new ImmutableFlightSessionConfig builder
   */
  public static ImmutableFlightSessionConfig.Builder builder() {
    return new ImmutableFlightSessionConfig.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableFlightSessionConfig ImmutableFlightSessionConfig}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "FlightSessionConfig", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements FlightSessionConfig.Builder {
    private @Nullable SessionConfig sessionConfig;
    private @Nullable BufferAllocator allocator;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code FlightSessionConfig} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(FlightSessionConfig instance) {
      Objects.requireNonNull(instance, "instance");
      Optional<SessionConfig> sessionConfigOptional = instance.sessionConfig();
      if (sessionConfigOptional.isPresent()) {
        sessionConfig(sessionConfigOptional);
      }
      Optional<BufferAllocator> allocatorOptional = instance.allocator();
      if (allocatorOptional.isPresent()) {
        allocator(allocatorOptional);
      }
      return this;
    }

    /**
     * Initializes the optional value {@link FlightSessionConfig#sessionConfig() sessionConfig} to sessionConfig.
     * @param sessionConfig The value for sessionConfig
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder sessionConfig(SessionConfig sessionConfig) {
      this.sessionConfig = Objects.requireNonNull(sessionConfig, "sessionConfig");
      return this;
    }

    /**
     * Initializes the optional value {@link FlightSessionConfig#sessionConfig() sessionConfig} to sessionConfig.
     * @param sessionConfig The value for sessionConfig
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder sessionConfig(Optional<? extends SessionConfig> sessionConfig) {
      this.sessionConfig = sessionConfig.orElse(null);
      return this;
    }

    /**
     * Initializes the optional value {@link FlightSessionConfig#allocator() allocator} to allocator.
     * @param allocator The value for allocator
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder allocator(BufferAllocator allocator) {
      this.allocator = Objects.requireNonNull(allocator, "allocator");
      return this;
    }

    /**
     * Initializes the optional value {@link FlightSessionConfig#allocator() allocator} to allocator.
     * @param allocator The value for allocator
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder allocator(Optional<? extends BufferAllocator> allocator) {
      this.allocator = allocator.orElse(null);
      return this;
    }

    /**
     * Builds a new {@link ImmutableFlightSessionConfig ImmutableFlightSessionConfig}.
     * @return An immutable instance of FlightSessionConfig
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableFlightSessionConfig build() {
      return new ImmutableFlightSessionConfig(sessionConfig, allocator);
    }
  }
}
