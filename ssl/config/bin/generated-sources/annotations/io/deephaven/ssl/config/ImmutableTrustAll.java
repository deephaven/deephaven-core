package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link TrustAll}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableTrustAll.builder()}.
 */
@Generated(from = "TrustAll", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableTrustAll extends TrustAll {

  private ImmutableTrustAll(ImmutableTrustAll.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableTrustAll} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableTrustAll
        && equalTo(0, (ImmutableTrustAll) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableTrustAll another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 191633814;
  }

  /**
   * Prints the immutable value {@code TrustAll}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "TrustAll{}";
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "TrustAll", generator = "Immutables")
  @Deprecated
  @JsonDeserialize
  @JsonTypeInfo(use=JsonTypeInfo.Id.NONE)
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends TrustAll {
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static ImmutableTrustAll fromJson(Json json) {
    ImmutableTrustAll.Builder builder = ImmutableTrustAll.builder();
    return builder.build();
  }

  /**
   * Creates an immutable copy of a {@link TrustAll} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable TrustAll instance
   */
  public static ImmutableTrustAll copyOf(TrustAll instance) {
    if (instance instanceof ImmutableTrustAll) {
      return (ImmutableTrustAll) instance;
    }
    return ImmutableTrustAll.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableTrustAll ImmutableTrustAll}.
   * <pre>
   * ImmutableTrustAll.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableTrustAll builder
   */
  public static ImmutableTrustAll.Builder builder() {
    return new ImmutableTrustAll.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableTrustAll ImmutableTrustAll}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "TrustAll", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code TrustAll} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(TrustAll instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableTrustAll ImmutableTrustAll}.
     * @return An immutable instance of TrustAll
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableTrustAll build() {
      return new ImmutableTrustAll(this);
    }
  }
}
