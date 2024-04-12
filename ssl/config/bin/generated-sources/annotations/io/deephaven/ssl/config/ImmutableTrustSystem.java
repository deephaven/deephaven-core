package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link TrustSystem}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableTrustSystem.builder()}.
 */
@Generated(from = "TrustSystem", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableTrustSystem extends TrustSystem {

  private ImmutableTrustSystem(ImmutableTrustSystem.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableTrustSystem} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableTrustSystem
        && equalTo(0, (ImmutableTrustSystem) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableTrustSystem another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 1478146744;
  }

  /**
   * Prints the immutable value {@code TrustSystem}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "TrustSystem{}";
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "TrustSystem", generator = "Immutables")
  @Deprecated
  @JsonDeserialize
  @JsonTypeInfo(use=JsonTypeInfo.Id.NONE)
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends TrustSystem {
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static ImmutableTrustSystem fromJson(Json json) {
    ImmutableTrustSystem.Builder builder = ImmutableTrustSystem.builder();
    return builder.build();
  }

  /**
   * Creates an immutable copy of a {@link TrustSystem} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable TrustSystem instance
   */
  public static ImmutableTrustSystem copyOf(TrustSystem instance) {
    if (instance instanceof ImmutableTrustSystem) {
      return (ImmutableTrustSystem) instance;
    }
    return ImmutableTrustSystem.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableTrustSystem ImmutableTrustSystem}.
   * <pre>
   * ImmutableTrustSystem.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableTrustSystem builder
   */
  public static ImmutableTrustSystem.Builder builder() {
    return new ImmutableTrustSystem.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableTrustSystem ImmutableTrustSystem}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "TrustSystem", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code TrustSystem} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(TrustSystem instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableTrustSystem ImmutableTrustSystem}.
     * @return An immutable instance of TrustSystem
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableTrustSystem build() {
      return new ImmutableTrustSystem(this);
    }
  }
}
