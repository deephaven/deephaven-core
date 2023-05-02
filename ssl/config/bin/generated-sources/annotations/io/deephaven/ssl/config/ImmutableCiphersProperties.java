package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link CiphersProperties}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableCiphersProperties.builder()}.
 */
@Generated(from = "CiphersProperties", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableCiphersProperties extends CiphersProperties {

  private ImmutableCiphersProperties(ImmutableCiphersProperties.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableCiphersProperties} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableCiphersProperties
        && equalTo(0, (ImmutableCiphersProperties) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableCiphersProperties another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return -884855380;
  }

  /**
   * Prints the immutable value {@code CiphersProperties}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "CiphersProperties{}";
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "CiphersProperties", generator = "Immutables")
  @Deprecated
  @JsonDeserialize
  @JsonTypeInfo(use=JsonTypeInfo.Id.NONE)
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends CiphersProperties {
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static ImmutableCiphersProperties fromJson(Json json) {
    ImmutableCiphersProperties.Builder builder = ImmutableCiphersProperties.builder();
    return builder.build();
  }

  /**
   * Creates an immutable copy of a {@link CiphersProperties} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable CiphersProperties instance
   */
  public static ImmutableCiphersProperties copyOf(CiphersProperties instance) {
    if (instance instanceof ImmutableCiphersProperties) {
      return (ImmutableCiphersProperties) instance;
    }
    return ImmutableCiphersProperties.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableCiphersProperties ImmutableCiphersProperties}.
   * <pre>
   * ImmutableCiphersProperties.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableCiphersProperties builder
   */
  public static ImmutableCiphersProperties.Builder builder() {
    return new ImmutableCiphersProperties.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableCiphersProperties ImmutableCiphersProperties}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "CiphersProperties", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code CiphersProperties} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(CiphersProperties instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableCiphersProperties ImmutableCiphersProperties}.
     * @return An immutable instance of CiphersProperties
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableCiphersProperties build() {
      return new ImmutableCiphersProperties(this);
    }
  }
}
