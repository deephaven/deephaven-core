package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link TrustJdk}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableTrustJdk.builder()}.
 */
@Generated(from = "TrustJdk", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableTrustJdk extends TrustJdk {

  private ImmutableTrustJdk(ImmutableTrustJdk.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableTrustJdk} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableTrustJdk
        && equalTo(0, (ImmutableTrustJdk) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableTrustJdk another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 191642214;
  }

  /**
   * Prints the immutable value {@code TrustJdk}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "TrustJdk{}";
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "TrustJdk", generator = "Immutables")
  @Deprecated
  @JsonDeserialize
  @JsonTypeInfo(use=JsonTypeInfo.Id.NONE)
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends TrustJdk {
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static ImmutableTrustJdk fromJson(Json json) {
    ImmutableTrustJdk.Builder builder = ImmutableTrustJdk.builder();
    return builder.build();
  }

  /**
   * Creates an immutable copy of a {@link TrustJdk} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable TrustJdk instance
   */
  public static ImmutableTrustJdk copyOf(TrustJdk instance) {
    if (instance instanceof ImmutableTrustJdk) {
      return (ImmutableTrustJdk) instance;
    }
    return ImmutableTrustJdk.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableTrustJdk ImmutableTrustJdk}.
   * <pre>
   * ImmutableTrustJdk.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableTrustJdk builder
   */
  public static ImmutableTrustJdk.Builder builder() {
    return new ImmutableTrustJdk.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableTrustJdk ImmutableTrustJdk}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "TrustJdk", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code TrustJdk} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(TrustJdk instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableTrustJdk ImmutableTrustJdk}.
     * @return An immutable instance of TrustJdk
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableTrustJdk build() {
      return new ImmutableTrustJdk(this);
    }
  }
}
