package io.deephaven.uri;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link QueryScopeUri}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableQueryScopeUri.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableQueryScopeUri.of()}.
 */
@Generated(from = "QueryScopeUri", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableQueryScopeUri extends QueryScopeUri {
  private final String variableName;

  private ImmutableQueryScopeUri(String variableName) {
    this.variableName = Objects.requireNonNull(variableName, "variableName");
  }

  private ImmutableQueryScopeUri(ImmutableQueryScopeUri original, String variableName) {
    this.variableName = variableName;
  }

  /**
   * The variable name.
   * @return the variable name
   */
  @Override
  public String variableName() {
    return variableName;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link QueryScopeUri#variableName() variableName} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for variableName
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableQueryScopeUri withVariableName(String value) {
    String newValue = Objects.requireNonNull(value, "variableName");
    if (this.variableName.equals(newValue)) return this;
    return validate(new ImmutableQueryScopeUri(this, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableQueryScopeUri} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableQueryScopeUri
        && equalTo(0, (ImmutableQueryScopeUri) another);
  }

  private boolean equalTo(int synthetic, ImmutableQueryScopeUri another) {
    return variableName.equals(another.variableName);
  }

  /**
   * Computes a hash code from attributes: {@code variableName}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + variableName.hashCode();
    return h;
  }

  /**
   * Construct a new immutable {@code QueryScopeUri} instance.
   * @param variableName The value for the {@code variableName} attribute
   * @return An immutable QueryScopeUri instance
   */
  public static ImmutableQueryScopeUri of(String variableName) {
    return validate(new ImmutableQueryScopeUri(variableName));
  }

  private static ImmutableQueryScopeUri validate(ImmutableQueryScopeUri instance) {
    instance.checkVariableName();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link QueryScopeUri} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable QueryScopeUri instance
   */
  public static ImmutableQueryScopeUri copyOf(QueryScopeUri instance) {
    if (instance instanceof ImmutableQueryScopeUri) {
      return (ImmutableQueryScopeUri) instance;
    }
    return ImmutableQueryScopeUri.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableQueryScopeUri ImmutableQueryScopeUri}.
   * <pre>
   * ImmutableQueryScopeUri.builder()
   *    .variableName(String) // required {@link QueryScopeUri#variableName() variableName}
   *    .build();
   * </pre>
   * @return A new ImmutableQueryScopeUri builder
   */
  public static ImmutableQueryScopeUri.Builder builder() {
    return new ImmutableQueryScopeUri.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableQueryScopeUri ImmutableQueryScopeUri}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "QueryScopeUri", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_VARIABLE_NAME = 0x1L;
    private long initBits = 0x1L;

    private String variableName;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code QueryScopeUri} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(QueryScopeUri instance) {
      Objects.requireNonNull(instance, "instance");
      variableName(instance.variableName());
      return this;
    }

    /**
     * Initializes the value for the {@link QueryScopeUri#variableName() variableName} attribute.
     * @param variableName The value for variableName 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder variableName(String variableName) {
      this.variableName = Objects.requireNonNull(variableName, "variableName");
      initBits &= ~INIT_BIT_VARIABLE_NAME;
      return this;
    }

    /**
     * Builds a new {@link ImmutableQueryScopeUri ImmutableQueryScopeUri}.
     * @return An immutable instance of QueryScopeUri
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableQueryScopeUri build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableQueryScopeUri.validate(new ImmutableQueryScopeUri(null, variableName));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_VARIABLE_NAME) != 0) attributes.add("variableName");
      return "Cannot build QueryScopeUri, some of required attributes are not set " + attributes;
    }
  }
}
