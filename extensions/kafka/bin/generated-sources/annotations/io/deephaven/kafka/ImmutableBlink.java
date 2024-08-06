package io.deephaven.kafka;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link KafkaTools.TableType.Blink}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableBlink.builder()}.
 */
@Generated(from = "KafkaTools.TableType.Blink", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableBlink extends KafkaTools.TableType.Blink {

  private ImmutableBlink(ImmutableBlink.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableBlink} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableBlink
        && equalTo(0, (ImmutableBlink) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableBlink another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 1954310880;
  }

  /**
   * Prints the immutable value {@code Blink}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "Blink{}";
  }

  /**
   * Creates an immutable copy of a {@link KafkaTools.TableType.Blink} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable Blink instance
   */
  public static ImmutableBlink copyOf(KafkaTools.TableType.Blink instance) {
    if (instance instanceof ImmutableBlink) {
      return (ImmutableBlink) instance;
    }
    return ImmutableBlink.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableBlink ImmutableBlink}.
   * <pre>
   * ImmutableBlink.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableBlink builder
   */
  public static ImmutableBlink.Builder builder() {
    return new ImmutableBlink.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableBlink ImmutableBlink}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "KafkaTools.TableType.Blink", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code Blink} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(KafkaTools.TableType.Blink instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableBlink ImmutableBlink}.
     * @return An immutable instance of Blink
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableBlink build() {
      return new ImmutableBlink(this);
    }
  }
}
