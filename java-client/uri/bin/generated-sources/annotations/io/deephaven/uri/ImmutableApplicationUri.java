package io.deephaven.uri;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ApplicationUri}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableApplicationUri.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableApplicationUri.of()}.
 */
@Generated(from = "ApplicationUri", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableApplicationUri extends ApplicationUri {
  private final String applicationId;
  private final String fieldName;

  private ImmutableApplicationUri(String applicationId, String fieldName) {
    this.applicationId = Objects.requireNonNull(applicationId, "applicationId");
    this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
  }

  private ImmutableApplicationUri(ImmutableApplicationUri original, String applicationId, String fieldName) {
    this.applicationId = applicationId;
    this.fieldName = fieldName;
  }

  /**
   * The application id.
   * @return the application id
   */
  @Override
  public String applicationId() {
    return applicationId;
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
   * Copy the current immutable object by setting a value for the {@link ApplicationUri#applicationId() applicationId} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for applicationId
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableApplicationUri withApplicationId(String value) {
    String newValue = Objects.requireNonNull(value, "applicationId");
    if (this.applicationId.equals(newValue)) return this;
    return validate(new ImmutableApplicationUri(this, newValue, this.fieldName));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ApplicationUri#fieldName() fieldName} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for fieldName
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableApplicationUri withFieldName(String value) {
    String newValue = Objects.requireNonNull(value, "fieldName");
    if (this.fieldName.equals(newValue)) return this;
    return validate(new ImmutableApplicationUri(this, this.applicationId, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableApplicationUri} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableApplicationUri
        && equalTo(0, (ImmutableApplicationUri) another);
  }

  private boolean equalTo(int synthetic, ImmutableApplicationUri another) {
    return applicationId.equals(another.applicationId)
        && fieldName.equals(another.fieldName);
  }

  /**
   * Computes a hash code from attributes: {@code applicationId}, {@code fieldName}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + applicationId.hashCode();
    h += (h << 5) + fieldName.hashCode();
    return h;
  }

  /**
   * Construct a new immutable {@code ApplicationUri} instance.
   * @param applicationId The value for the {@code applicationId} attribute
   * @param fieldName The value for the {@code fieldName} attribute
   * @return An immutable ApplicationUri instance
   */
  public static ImmutableApplicationUri of(String applicationId, String fieldName) {
    return validate(new ImmutableApplicationUri(applicationId, fieldName));
  }

  private static ImmutableApplicationUri validate(ImmutableApplicationUri instance) {
    instance.checkFieldName();
    instance.checkApplicationId();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link ApplicationUri} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ApplicationUri instance
   */
  public static ImmutableApplicationUri copyOf(ApplicationUri instance) {
    if (instance instanceof ImmutableApplicationUri) {
      return (ImmutableApplicationUri) instance;
    }
    return ImmutableApplicationUri.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableApplicationUri ImmutableApplicationUri}.
   * <pre>
   * ImmutableApplicationUri.builder()
   *    .applicationId(String) // required {@link ApplicationUri#applicationId() applicationId}
   *    .fieldName(String) // required {@link ApplicationUri#fieldName() fieldName}
   *    .build();
   * </pre>
   * @return A new ImmutableApplicationUri builder
   */
  public static ImmutableApplicationUri.Builder builder() {
    return new ImmutableApplicationUri.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableApplicationUri ImmutableApplicationUri}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ApplicationUri", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_APPLICATION_ID = 0x1L;
    private static final long INIT_BIT_FIELD_NAME = 0x2L;
    private long initBits = 0x3L;

    private String applicationId;
    private String fieldName;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ApplicationUri} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(ApplicationUri instance) {
      Objects.requireNonNull(instance, "instance");
      applicationId(instance.applicationId());
      fieldName(instance.fieldName());
      return this;
    }

    /**
     * Initializes the value for the {@link ApplicationUri#applicationId() applicationId} attribute.
     * @param applicationId The value for applicationId 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder applicationId(String applicationId) {
      this.applicationId = Objects.requireNonNull(applicationId, "applicationId");
      initBits &= ~INIT_BIT_APPLICATION_ID;
      return this;
    }

    /**
     * Initializes the value for the {@link ApplicationUri#fieldName() fieldName} attribute.
     * @param fieldName The value for fieldName 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder fieldName(String fieldName) {
      this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
      initBits &= ~INIT_BIT_FIELD_NAME;
      return this;
    }

    /**
     * Builds a new {@link ImmutableApplicationUri ImmutableApplicationUri}.
     * @return An immutable instance of ApplicationUri
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableApplicationUri build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableApplicationUri.validate(new ImmutableApplicationUri(null, applicationId, fieldName));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_APPLICATION_ID) != 0) attributes.add("applicationId");
      if ((initBits & INIT_BIT_FIELD_NAME) != 0) attributes.add("fieldName");
      return "Cannot build ApplicationUri, some of required attributes are not set " + attributes;
    }
  }
}
