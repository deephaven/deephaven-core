package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link CiphersModern}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableCiphersModern.builder()}.
 */
@Generated(from = "CiphersModern", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableCiphersModern extends CiphersModern {

  private ImmutableCiphersModern(ImmutableCiphersModern.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableCiphersModern} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableCiphersModern
        && equalTo(0, (ImmutableCiphersModern) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableCiphersModern another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return -1635628904;
  }

  /**
   * Prints the immutable value {@code CiphersModern}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "CiphersModern{}";
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "CiphersModern", generator = "Immutables")
  @Deprecated
  @JsonDeserialize
  @JsonTypeInfo(use=JsonTypeInfo.Id.NONE)
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends CiphersModern {
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static ImmutableCiphersModern fromJson(Json json) {
    ImmutableCiphersModern.Builder builder = ImmutableCiphersModern.builder();
    return builder.build();
  }

  /**
   * Creates an immutable copy of a {@link CiphersModern} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable CiphersModern instance
   */
  public static ImmutableCiphersModern copyOf(CiphersModern instance) {
    if (instance instanceof ImmutableCiphersModern) {
      return (ImmutableCiphersModern) instance;
    }
    return ImmutableCiphersModern.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableCiphersModern ImmutableCiphersModern}.
   * <pre>
   * ImmutableCiphersModern.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableCiphersModern builder
   */
  public static ImmutableCiphersModern.Builder builder() {
    return new ImmutableCiphersModern.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableCiphersModern ImmutableCiphersModern}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "CiphersModern", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code CiphersModern} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(CiphersModern instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableCiphersModern ImmutableCiphersModern}.
     * @return An immutable instance of CiphersModern
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableCiphersModern build() {
      return new ImmutableCiphersModern(this);
    }
  }
}
