package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link IdentityProperties}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableIdentityProperties.builder()}.
 */
@Generated(from = "IdentityProperties", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableIdentityProperties extends IdentityProperties {

  private ImmutableIdentityProperties(ImmutableIdentityProperties.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableIdentityProperties} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableIdentityProperties
        && equalTo(0, (ImmutableIdentityProperties) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableIdentityProperties another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return -612577154;
  }

  /**
   * Prints the immutable value {@code IdentityProperties}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "IdentityProperties{}";
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "IdentityProperties", generator = "Immutables")
  @Deprecated
  @JsonDeserialize
  @JsonTypeInfo(use=JsonTypeInfo.Id.NONE)
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends IdentityProperties {
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static ImmutableIdentityProperties fromJson(Json json) {
    ImmutableIdentityProperties.Builder builder = ImmutableIdentityProperties.builder();
    return builder.build();
  }

  /**
   * Creates an immutable copy of a {@link IdentityProperties} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable IdentityProperties instance
   */
  public static ImmutableIdentityProperties copyOf(IdentityProperties instance) {
    if (instance instanceof ImmutableIdentityProperties) {
      return (ImmutableIdentityProperties) instance;
    }
    return ImmutableIdentityProperties.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableIdentityProperties ImmutableIdentityProperties}.
   * <pre>
   * ImmutableIdentityProperties.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableIdentityProperties builder
   */
  public static ImmutableIdentityProperties.Builder builder() {
    return new ImmutableIdentityProperties.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableIdentityProperties ImmutableIdentityProperties}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "IdentityProperties", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code IdentityProperties} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(IdentityProperties instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableIdentityProperties ImmutableIdentityProperties}.
     * @return An immutable instance of IdentityProperties
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableIdentityProperties build() {
      return new ImmutableIdentityProperties(this);
    }
  }
}
