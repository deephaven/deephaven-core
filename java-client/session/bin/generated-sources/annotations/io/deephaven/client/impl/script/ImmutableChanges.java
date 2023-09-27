package io.deephaven.client.impl.script;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link Changes}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableChanges.builder()}.
 */
@Generated(from = "Changes", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableChanges extends Changes {
  private final @Nullable String errorMessage;
  private final FieldChanges changes;

  private ImmutableChanges(
      @Nullable String errorMessage,
      FieldChanges changes) {
    this.errorMessage = errorMessage;
    this.changes = changes;
  }

  /**
   * @return The value of the {@code errorMessage} attribute
   */
  @Override
  public Optional<String> errorMessage() {
    return Optional.ofNullable(errorMessage);
  }

  /**
   * @return The value of the {@code changes} attribute
   */
  @Override
  public FieldChanges changes() {
    return changes;
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link Changes#errorMessage() errorMessage} attribute.
   * @param value The value for errorMessage
   * @return A modified copy of {@code this} object
   */
  public final ImmutableChanges withErrorMessage(String value) {
    @Nullable String newValue = Objects.requireNonNull(value, "errorMessage");
    if (Objects.equals(this.errorMessage, newValue)) return this;
    return new ImmutableChanges(newValue, this.changes);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link Changes#errorMessage() errorMessage} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for errorMessage
   * @return A modified copy of {@code this} object
   */
  public final ImmutableChanges withErrorMessage(Optional<String> optional) {
    @Nullable String value = optional.orElse(null);
    if (Objects.equals(this.errorMessage, value)) return this;
    return new ImmutableChanges(value, this.changes);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link Changes#changes() changes} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for changes
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableChanges withChanges(FieldChanges value) {
    if (this.changes == value) return this;
    FieldChanges newValue = Objects.requireNonNull(value, "changes");
    return new ImmutableChanges(this.errorMessage, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableChanges} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableChanges
        && equalTo(0, (ImmutableChanges) another);
  }

  private boolean equalTo(int synthetic, ImmutableChanges another) {
    return Objects.equals(errorMessage, another.errorMessage)
        && changes.equals(another.changes);
  }

  /**
   * Computes a hash code from attributes: {@code errorMessage}, {@code changes}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + Objects.hashCode(errorMessage);
    h += (h << 5) + changes.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code Changes} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("Changes")
        .omitNullValues()
        .add("errorMessage", errorMessage)
        .add("changes", changes)
        .toString();
  }

  /**
   * Creates an immutable copy of a {@link Changes} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable Changes instance
   */
  public static ImmutableChanges copyOf(Changes instance) {
    if (instance instanceof ImmutableChanges) {
      return (ImmutableChanges) instance;
    }
    return ImmutableChanges.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableChanges ImmutableChanges}.
   * <pre>
   * ImmutableChanges.builder()
   *    .errorMessage(String) // optional {@link Changes#errorMessage() errorMessage}
   *    .changes(io.deephaven.client.impl.script.FieldChanges) // required {@link Changes#changes() changes}
   *    .build();
   * </pre>
   * @return A new ImmutableChanges builder
   */
  public static ImmutableChanges.Builder builder() {
    return new ImmutableChanges.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableChanges ImmutableChanges}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "Changes", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements Changes.Builder {
    private static final long INIT_BIT_CHANGES = 0x1L;
    private long initBits = 0x1L;

    private @Nullable String errorMessage;
    private @Nullable FieldChanges changes;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code Changes} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(Changes instance) {
      Objects.requireNonNull(instance, "instance");
      Optional<String> errorMessageOptional = instance.errorMessage();
      if (errorMessageOptional.isPresent()) {
        errorMessage(errorMessageOptional);
      }
      changes(instance.changes());
      return this;
    }

    /**
     * Initializes the optional value {@link Changes#errorMessage() errorMessage} to errorMessage.
     * @param errorMessage The value for errorMessage
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder errorMessage(String errorMessage) {
      this.errorMessage = Objects.requireNonNull(errorMessage, "errorMessage");
      return this;
    }

    /**
     * Initializes the optional value {@link Changes#errorMessage() errorMessage} to errorMessage.
     * @param errorMessage The value for errorMessage
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder errorMessage(Optional<String> errorMessage) {
      this.errorMessage = errorMessage.orElse(null);
      return this;
    }

    /**
     * Initializes the value for the {@link Changes#changes() changes} attribute.
     * @param changes The value for changes 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder changes(FieldChanges changes) {
      this.changes = Objects.requireNonNull(changes, "changes");
      initBits &= ~INIT_BIT_CHANGES;
      return this;
    }

    /**
     * Builds a new {@link ImmutableChanges ImmutableChanges}.
     * @return An immutable instance of Changes
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableChanges build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableChanges(errorMessage, changes);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_CHANGES) != 0) attributes.add("changes");
      return "Cannot build Changes, some of required attributes are not set " + attributes;
    }
  }
}
