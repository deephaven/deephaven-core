package io.deephaven.server.runner;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link DeephavenApiServerTestConfig}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableDeephavenApiServerTestConfig.builder()}.
 */
@Generated(from = "DeephavenApiServerTestConfig", generator = "Immutables")
@SuppressWarnings({"all"})
@SuppressFBWarnings
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableDeephavenApiServerTestConfig
    extends DeephavenApiServerTestConfig {

  private ImmutableDeephavenApiServerTestConfig(ImmutableDeephavenApiServerTestConfig.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableDeephavenApiServerTestConfig} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableDeephavenApiServerTestConfig
        && equalTo(0, (ImmutableDeephavenApiServerTestConfig) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableDeephavenApiServerTestConfig another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 51896325;
  }

  /**
   * Prints the immutable value {@code DeephavenApiServerTestConfig}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "DeephavenApiServerTestConfig{}";
  }

  /**
   * Creates an immutable copy of a {@link DeephavenApiServerTestConfig} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable DeephavenApiServerTestConfig instance
   */
  public static ImmutableDeephavenApiServerTestConfig copyOf(DeephavenApiServerTestConfig instance) {
    if (instance instanceof ImmutableDeephavenApiServerTestConfig) {
      return (ImmutableDeephavenApiServerTestConfig) instance;
    }
    return ImmutableDeephavenApiServerTestConfig.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableDeephavenApiServerTestConfig ImmutableDeephavenApiServerTestConfig}.
   * <pre>
   * ImmutableDeephavenApiServerTestConfig.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableDeephavenApiServerTestConfig builder
   */
  public static ImmutableDeephavenApiServerTestConfig.Builder builder() {
    return new ImmutableDeephavenApiServerTestConfig.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableDeephavenApiServerTestConfig ImmutableDeephavenApiServerTestConfig}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "DeephavenApiServerTestConfig", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements DeephavenApiServerTestConfig.Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code DeephavenApiServerTestConfig} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(DeephavenApiServerTestConfig instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableDeephavenApiServerTestConfig ImmutableDeephavenApiServerTestConfig}.
     * @return An immutable instance of DeephavenApiServerTestConfig
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableDeephavenApiServerTestConfig build() {
      return new ImmutableDeephavenApiServerTestConfig(this);
    }
  }
}
