package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link CiphersIntermediate}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableCiphersIntermediate.builder()}.
 */
@Generated(from = "CiphersIntermediate", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableCiphersIntermediate extends CiphersIntermediate {

  private ImmutableCiphersIntermediate(ImmutableCiphersIntermediate.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableCiphersIntermediate} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableCiphersIntermediate
        && equalTo(0, (ImmutableCiphersIntermediate) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableCiphersIntermediate another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 76526322;
  }

  /**
   * Prints the immutable value {@code CiphersIntermediate}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "CiphersIntermediate{}";
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "CiphersIntermediate", generator = "Immutables")
  @Deprecated
  @JsonDeserialize
  @JsonTypeInfo(use=JsonTypeInfo.Id.NONE)
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends CiphersIntermediate {
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static ImmutableCiphersIntermediate fromJson(Json json) {
    ImmutableCiphersIntermediate.Builder builder = ImmutableCiphersIntermediate.builder();
    return builder.build();
  }

  /**
   * Creates an immutable copy of a {@link CiphersIntermediate} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable CiphersIntermediate instance
   */
  public static ImmutableCiphersIntermediate copyOf(CiphersIntermediate instance) {
    if (instance instanceof ImmutableCiphersIntermediate) {
      return (ImmutableCiphersIntermediate) instance;
    }
    return ImmutableCiphersIntermediate.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableCiphersIntermediate ImmutableCiphersIntermediate}.
   * <pre>
   * ImmutableCiphersIntermediate.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableCiphersIntermediate builder
   */
  public static ImmutableCiphersIntermediate.Builder builder() {
    return new ImmutableCiphersIntermediate.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableCiphersIntermediate ImmutableCiphersIntermediate}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "CiphersIntermediate", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code CiphersIntermediate} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(CiphersIntermediate instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableCiphersIntermediate ImmutableCiphersIntermediate}.
     * @return An immutable instance of CiphersIntermediate
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableCiphersIntermediate build() {
      return new ImmutableCiphersIntermediate(this);
    }
  }
}
