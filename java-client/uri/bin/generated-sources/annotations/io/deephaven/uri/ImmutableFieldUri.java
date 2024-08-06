package io.deephaven.uri;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link FieldUri}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableFieldUri.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableFieldUri.of()}.
 */
@Generated(from = "FieldUri", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableFieldUri extends FieldUri {
  private final String fieldName;

  private ImmutableFieldUri(String fieldName) {
    this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
  }

  private ImmutableFieldUri(ImmutableFieldUri original, String fieldName) {
    this.fieldName = fieldName;
  }

  /**
   * The field name.
   * @return the field name
   */
  @Override
  public String fieldName() {
    return fieldName;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link FieldUri#fieldName() fieldName} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for fieldName
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableFieldUri withFieldName(String value) {
    String newValue = Objects.requireNonNull(value, "fieldName");
    if (this.fieldName.equals(newValue)) return this;
    return validate(new ImmutableFieldUri(this, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableFieldUri} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableFieldUri
        && equalTo(0, (ImmutableFieldUri) another);
  }

  private boolean equalTo(int synthetic, ImmutableFieldUri another) {
    return fieldName.equals(another.fieldName);
  }

  /**
   * Computes a hash code from attributes: {@code fieldName}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + fieldName.hashCode();
    return h;
  }

  /**
   * Construct a new immutable {@code FieldUri} instance.
   * @param fieldName The value for the {@code fieldName} attribute
   * @return An immutable FieldUri instance
   */
  public static ImmutableFieldUri of(String fieldName) {
    return validate(new ImmutableFieldUri(fieldName));
  }

  private static ImmutableFieldUri validate(ImmutableFieldUri instance) {
    instance.checkFieldName();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link FieldUri} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable FieldUri instance
   */
  public static ImmutableFieldUri copyOf(FieldUri instance) {
    if (instance instanceof ImmutableFieldUri) {
      return (ImmutableFieldUri) instance;
    }
    return ImmutableFieldUri.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableFieldUri ImmutableFieldUri}.
   * <pre>
   * ImmutableFieldUri.builder()
   *    .fieldName(String) // required {@link FieldUri#fieldName() fieldName}
   *    .build();
   * </pre>
   * @return A new ImmutableFieldUri builder
   */
  public static ImmutableFieldUri.Builder builder() {
    return new ImmutableFieldUri.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableFieldUri ImmutableFieldUri}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "FieldUri", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_FIELD_NAME = 0x1L;
    private long initBits = 0x1L;

    private String fieldName;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code FieldUri} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(FieldUri instance) {
      Objects.requireNonNull(instance, "instance");
      fieldName(instance.fieldName());
      return this;
    }

    /**
     * Initializes the value for the {@link FieldUri#fieldName() fieldName} attribute.
     * @param fieldName The value for fieldName 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder fieldName(String fieldName) {
      this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
      initBits &= ~INIT_BIT_FIELD_NAME;
      return this;
    }

    /**
     * Builds a new {@link ImmutableFieldUri ImmutableFieldUri}.
     * @return An immutable instance of FieldUri
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableFieldUri build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableFieldUri.validate(new ImmutableFieldUri(null, fieldName));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_FIELD_NAME) != 0) attributes.add("fieldName");
      return "Cannot build FieldUri, some of required attributes are not set " + attributes;
    }
  }
}
