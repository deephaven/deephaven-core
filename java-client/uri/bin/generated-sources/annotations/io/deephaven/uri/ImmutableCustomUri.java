package io.deephaven.uri;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link CustomUri}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableCustomUri.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableCustomUri.of()}.
 */
@Generated(from = "CustomUri", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableCustomUri extends CustomUri {
  private final URI uri;

  private ImmutableCustomUri(URI uri) {
    this.uri = Objects.requireNonNull(uri, "uri");
  }

  private ImmutableCustomUri(ImmutableCustomUri original, URI uri) {
    this.uri = uri;
  }

  /**
   * @return The value of the {@code uri} attribute
   */
  @Override
  public URI uri() {
    return uri;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CustomUri#uri() uri} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for uri
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCustomUri withUri(URI value) {
    if (this.uri == value) return this;
    URI newValue = Objects.requireNonNull(value, "uri");
    return validate(new ImmutableCustomUri(this, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableCustomUri} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableCustomUri
        && equalTo(0, (ImmutableCustomUri) another);
  }

  private boolean equalTo(int synthetic, ImmutableCustomUri another) {
    return uri.equals(another.uri);
  }

  /**
   * Computes a hash code from attributes: {@code uri}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + uri.hashCode();
    return h;
  }

  /**
   * Construct a new immutable {@code CustomUri} instance.
   * @param uri The value for the {@code uri} attribute
   * @return An immutable CustomUri instance
   */
  public static ImmutableCustomUri of(URI uri) {
    return validate(new ImmutableCustomUri(uri));
  }

  private static ImmutableCustomUri validate(ImmutableCustomUri instance) {
    instance.checkScheme();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link CustomUri} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable CustomUri instance
   */
  public static ImmutableCustomUri copyOf(CustomUri instance) {
    if (instance instanceof ImmutableCustomUri) {
      return (ImmutableCustomUri) instance;
    }
    return ImmutableCustomUri.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableCustomUri ImmutableCustomUri}.
   * <pre>
   * ImmutableCustomUri.builder()
   *    .uri(java.net.URI) // required {@link CustomUri#uri() uri}
   *    .build();
   * </pre>
   * @return A new ImmutableCustomUri builder
   */
  public static ImmutableCustomUri.Builder builder() {
    return new ImmutableCustomUri.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableCustomUri ImmutableCustomUri}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "CustomUri", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_URI = 0x1L;
    private long initBits = 0x1L;

    private URI uri;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code CustomUri} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(CustomUri instance) {
      Objects.requireNonNull(instance, "instance");
      uri(instance.uri());
      return this;
    }

    /**
     * Initializes the value for the {@link CustomUri#uri() uri} attribute.
     * @param uri The value for uri 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder uri(URI uri) {
      this.uri = Objects.requireNonNull(uri, "uri");
      initBits &= ~INIT_BIT_URI;
      return this;
    }

    /**
     * Builds a new {@link ImmutableCustomUri ImmutableCustomUri}.
     * @return An immutable instance of CustomUri
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableCustomUri build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableCustomUri.validate(new ImmutableCustomUri(null, uri));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_URI) != 0) attributes.add("uri");
      return "Cannot build CustomUri, some of required attributes are not set " + attributes;
    }
  }
}
