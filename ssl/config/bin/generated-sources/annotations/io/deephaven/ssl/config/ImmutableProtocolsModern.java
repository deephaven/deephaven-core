package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ProtocolsModern}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableProtocolsModern.builder()}.
 */
@Generated(from = "ProtocolsModern", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableProtocolsModern extends ProtocolsModern {

  private ImmutableProtocolsModern(ImmutableProtocolsModern.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableProtocolsModern} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableProtocolsModern
        && equalTo(0, (ImmutableProtocolsModern) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableProtocolsModern another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 1720140523;
  }

  /**
   * Prints the immutable value {@code ProtocolsModern}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ProtocolsModern{}";
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "ProtocolsModern", generator = "Immutables")
  @Deprecated
  @JsonDeserialize
  @JsonTypeInfo(use=JsonTypeInfo.Id.NONE)
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends ProtocolsModern {
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static ImmutableProtocolsModern fromJson(Json json) {
    ImmutableProtocolsModern.Builder builder = ImmutableProtocolsModern.builder();
    return builder.build();
  }

  /**
   * Creates an immutable copy of a {@link ProtocolsModern} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ProtocolsModern instance
   */
  public static ImmutableProtocolsModern copyOf(ProtocolsModern instance) {
    if (instance instanceof ImmutableProtocolsModern) {
      return (ImmutableProtocolsModern) instance;
    }
    return ImmutableProtocolsModern.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableProtocolsModern ImmutableProtocolsModern}.
   * <pre>
   * ImmutableProtocolsModern.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableProtocolsModern builder
   */
  public static ImmutableProtocolsModern.Builder builder() {
    return new ImmutableProtocolsModern.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableProtocolsModern ImmutableProtocolsModern}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ProtocolsModern", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ProtocolsModern} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(ProtocolsModern instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableProtocolsModern ImmutableProtocolsModern}.
     * @return An immutable instance of ProtocolsModern
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableProtocolsModern build() {
      return new ImmutableProtocolsModern(this);
    }
  }
}
