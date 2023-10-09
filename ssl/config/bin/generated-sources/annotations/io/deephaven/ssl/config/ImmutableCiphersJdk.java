package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link CiphersJdk}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableCiphersJdk.builder()}.
 */
@Generated(from = "CiphersJdk", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableCiphersJdk extends CiphersJdk {

  private ImmutableCiphersJdk(ImmutableCiphersJdk.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableCiphersJdk} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableCiphersJdk
        && equalTo(0, (ImmutableCiphersJdk) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableCiphersJdk another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 223693686;
  }

  /**
   * Prints the immutable value {@code CiphersJdk}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "CiphersJdk{}";
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "CiphersJdk", generator = "Immutables")
  @Deprecated
  @JsonDeserialize
  @JsonTypeInfo(use=JsonTypeInfo.Id.NONE)
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends CiphersJdk {
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static ImmutableCiphersJdk fromJson(Json json) {
    ImmutableCiphersJdk.Builder builder = ImmutableCiphersJdk.builder();
    return builder.build();
  }

  /**
   * Creates an immutable copy of a {@link CiphersJdk} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable CiphersJdk instance
   */
  public static ImmutableCiphersJdk copyOf(CiphersJdk instance) {
    if (instance instanceof ImmutableCiphersJdk) {
      return (ImmutableCiphersJdk) instance;
    }
    return ImmutableCiphersJdk.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableCiphersJdk ImmutableCiphersJdk}.
   * <pre>
   * ImmutableCiphersJdk.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableCiphersJdk builder
   */
  public static ImmutableCiphersJdk.Builder builder() {
    return new ImmutableCiphersJdk.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableCiphersJdk ImmutableCiphersJdk}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "CiphersJdk", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code CiphersJdk} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(CiphersJdk instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableCiphersJdk ImmutableCiphersJdk}.
     * @return An immutable instance of CiphersJdk
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableCiphersJdk build() {
      return new ImmutableCiphersJdk(this);
    }
  }
}
