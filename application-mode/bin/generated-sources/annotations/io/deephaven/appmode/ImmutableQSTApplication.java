package io.deephaven.appmode;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link QSTApplication}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableQSTApplication.builder()}.
 */
@Generated(from = "QSTApplication", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableQSTApplication extends QSTApplication {

  private ImmutableQSTApplication(ImmutableQSTApplication.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableQSTApplication} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableQSTApplication
        && equalTo(0, (ImmutableQSTApplication) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableQSTApplication another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return -1725180637;
  }

  /**
   * Prints the immutable value {@code QSTApplication}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "QSTApplication{}";
  }

  /**
   * Creates an immutable copy of a {@link QSTApplication} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable QSTApplication instance
   */
  public static ImmutableQSTApplication copyOf(QSTApplication instance) {
    if (instance instanceof ImmutableQSTApplication) {
      return (ImmutableQSTApplication) instance;
    }
    return ImmutableQSTApplication.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableQSTApplication ImmutableQSTApplication}.
   * <pre>
   * ImmutableQSTApplication.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableQSTApplication builder
   */
  public static ImmutableQSTApplication.Builder builder() {
    return new ImmutableQSTApplication.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableQSTApplication ImmutableQSTApplication}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "QSTApplication", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code QSTApplication} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(QSTApplication instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableQSTApplication ImmutableQSTApplication}.
     * @return An immutable instance of QSTApplication
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableQSTApplication build() {
      return new ImmutableQSTApplication(this);
    }
  }
}
