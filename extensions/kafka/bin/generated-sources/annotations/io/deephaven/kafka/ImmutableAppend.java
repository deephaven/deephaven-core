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
 * Immutable implementation of {@link KafkaTools.TableType.Append}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAppend.builder()}.
 */
@Generated(from = "KafkaTools.TableType.Append", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableAppend extends KafkaTools.TableType.Append {

  private ImmutableAppend(ImmutableAppend.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAppend} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAppend
        && equalTo(0, (ImmutableAppend) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableAppend another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 429359220;
  }

  /**
   * Prints the immutable value {@code Append}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "Append{}";
  }

  /**
   * Creates an immutable copy of a {@link KafkaTools.TableType.Append} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable Append instance
   */
  public static ImmutableAppend copyOf(KafkaTools.TableType.Append instance) {
    if (instance instanceof ImmutableAppend) {
      return (ImmutableAppend) instance;
    }
    return ImmutableAppend.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAppend ImmutableAppend}.
   * <pre>
   * ImmutableAppend.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableAppend builder
   */
  public static ImmutableAppend.Builder builder() {
    return new ImmutableAppend.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAppend ImmutableAppend}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "KafkaTools.TableType.Append", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code Append} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(KafkaTools.TableType.Append instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableAppend ImmutableAppend}.
     * @return An immutable instance of Append
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAppend build() {
      return new ImmutableAppend(this);
    }
  }
}
