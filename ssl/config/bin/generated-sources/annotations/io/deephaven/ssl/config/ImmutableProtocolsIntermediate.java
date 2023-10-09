package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ProtocolsIntermediate}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableProtocolsIntermediate.builder()}.
 */
@Generated(from = "ProtocolsIntermediate", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableProtocolsIntermediate extends ProtocolsIntermediate {

  private ImmutableProtocolsIntermediate(ImmutableProtocolsIntermediate.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableProtocolsIntermediate} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableProtocolsIntermediate
        && equalTo(0, (ImmutableProtocolsIntermediate) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableProtocolsIntermediate another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return -1240046843;
  }

  /**
   * Prints the immutable value {@code ProtocolsIntermediate}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ProtocolsIntermediate{}";
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "ProtocolsIntermediate", generator = "Immutables")
  @Deprecated
  @JsonDeserialize
  @JsonTypeInfo(use=JsonTypeInfo.Id.NONE)
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends ProtocolsIntermediate {
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static ImmutableProtocolsIntermediate fromJson(Json json) {
    ImmutableProtocolsIntermediate.Builder builder = ImmutableProtocolsIntermediate.builder();
    return builder.build();
  }

  /**
   * Creates an immutable copy of a {@link ProtocolsIntermediate} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ProtocolsIntermediate instance
   */
  public static ImmutableProtocolsIntermediate copyOf(ProtocolsIntermediate instance) {
    if (instance instanceof ImmutableProtocolsIntermediate) {
      return (ImmutableProtocolsIntermediate) instance;
    }
    return ImmutableProtocolsIntermediate.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableProtocolsIntermediate ImmutableProtocolsIntermediate}.
   * <pre>
   * ImmutableProtocolsIntermediate.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableProtocolsIntermediate builder
   */
  public static ImmutableProtocolsIntermediate.Builder builder() {
    return new ImmutableProtocolsIntermediate.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableProtocolsIntermediate ImmutableProtocolsIntermediate}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ProtocolsIntermediate", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ProtocolsIntermediate} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(ProtocolsIntermediate instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableProtocolsIntermediate ImmutableProtocolsIntermediate}.
     * @return An immutable instance of ProtocolsIntermediate
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableProtocolsIntermediate build() {
      return new ImmutableProtocolsIntermediate(this);
    }
  }
}
