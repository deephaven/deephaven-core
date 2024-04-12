package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link TrustProperties}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableTrustProperties.builder()}.
 */
@Generated(from = "TrustProperties", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableTrustProperties extends TrustProperties {

  private ImmutableTrustProperties(ImmutableTrustProperties.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableTrustProperties} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableTrustProperties
        && equalTo(0, (ImmutableTrustProperties) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableTrustProperties another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return -1556246340;
  }

  /**
   * Prints the immutable value {@code TrustProperties}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "TrustProperties{}";
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "TrustProperties", generator = "Immutables")
  @Deprecated
  @JsonDeserialize
  @JsonTypeInfo(use=JsonTypeInfo.Id.NONE)
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends TrustProperties {
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static ImmutableTrustProperties fromJson(Json json) {
    ImmutableTrustProperties.Builder builder = ImmutableTrustProperties.builder();
    return builder.build();
  }

  /**
   * Creates an immutable copy of a {@link TrustProperties} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable TrustProperties instance
   */
  public static ImmutableTrustProperties copyOf(TrustProperties instance) {
    if (instance instanceof ImmutableTrustProperties) {
      return (ImmutableTrustProperties) instance;
    }
    return ImmutableTrustProperties.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableTrustProperties ImmutableTrustProperties}.
   * <pre>
   * ImmutableTrustProperties.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableTrustProperties builder
   */
  public static ImmutableTrustProperties.Builder builder() {
    return new ImmutableTrustProperties.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableTrustProperties ImmutableTrustProperties}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "TrustProperties", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code TrustProperties} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(TrustProperties instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableTrustProperties ImmutableTrustProperties}.
     * @return An immutable instance of TrustProperties
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableTrustProperties build() {
      return new ImmutableTrustProperties(this);
    }
  }
}
