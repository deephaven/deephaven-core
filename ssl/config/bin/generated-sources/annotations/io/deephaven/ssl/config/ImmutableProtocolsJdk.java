package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ProtocolsJdk}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableProtocolsJdk.builder()}.
 */
@Generated(from = "ProtocolsJdk", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableProtocolsJdk extends ProtocolsJdk {

  private ImmutableProtocolsJdk(ImmutableProtocolsJdk.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableProtocolsJdk} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableProtocolsJdk
        && equalTo(0, (ImmutableProtocolsJdk) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableProtocolsJdk another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return -1668568573;
  }

  /**
   * Prints the immutable value {@code ProtocolsJdk}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ProtocolsJdk{}";
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "ProtocolsJdk", generator = "Immutables")
  @Deprecated
  @JsonDeserialize
  @JsonTypeInfo(use=JsonTypeInfo.Id.NONE)
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends ProtocolsJdk {
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static ImmutableProtocolsJdk fromJson(Json json) {
    ImmutableProtocolsJdk.Builder builder = ImmutableProtocolsJdk.builder();
    return builder.build();
  }

  /**
   * Creates an immutable copy of a {@link ProtocolsJdk} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ProtocolsJdk instance
   */
  public static ImmutableProtocolsJdk copyOf(ProtocolsJdk instance) {
    if (instance instanceof ImmutableProtocolsJdk) {
      return (ImmutableProtocolsJdk) instance;
    }
    return ImmutableProtocolsJdk.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableProtocolsJdk ImmutableProtocolsJdk}.
   * <pre>
   * ImmutableProtocolsJdk.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableProtocolsJdk builder
   */
  public static ImmutableProtocolsJdk.Builder builder() {
    return new ImmutableProtocolsJdk.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableProtocolsJdk ImmutableProtocolsJdk}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ProtocolsJdk", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ProtocolsJdk} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(ProtocolsJdk instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableProtocolsJdk ImmutableProtocolsJdk}.
     * @return An immutable instance of ProtocolsJdk
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableProtocolsJdk build() {
      return new ImmutableProtocolsJdk(this);
    }
  }
}
