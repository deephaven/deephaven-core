package io.deephaven.uri;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link RemoteUri}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableRemoteUri.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableRemoteUri.of()}.
 */
@Generated(from = "RemoteUri", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableRemoteUri extends RemoteUri {
  private final DeephavenTarget target;
  private final StructuredUri uri;

  private ImmutableRemoteUri(DeephavenTarget target, StructuredUri uri) {
    this.target = Objects.requireNonNull(target, "target");
    this.uri = Objects.requireNonNull(uri, "uri");
  }

  private ImmutableRemoteUri(
      ImmutableRemoteUri original,
      DeephavenTarget target,
      StructuredUri uri) {
    this.target = target;
    this.uri = uri;
  }

  /**
   * The Deephaven target.
   * @return the target
   */
  @Override
  public DeephavenTarget target() {
    return target;
  }

  /**
   * The <em>inner</em> URI. As opposed to {@link #toURI()}, which represents {@code this} as a URI.
   * @return the inner URI
   */
  @Override
  public StructuredUri uri() {
    return uri;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link RemoteUri#target() target} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for target
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRemoteUri withTarget(DeephavenTarget value) {
    if (this.target == value) return this;
    DeephavenTarget newValue = Objects.requireNonNull(value, "target");
    return new ImmutableRemoteUri(this, newValue, this.uri);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link RemoteUri#uri() uri} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for uri
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRemoteUri withUri(StructuredUri value) {
    if (this.uri == value) return this;
    StructuredUri newValue = Objects.requireNonNull(value, "uri");
    return new ImmutableRemoteUri(this, this.target, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableRemoteUri} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableRemoteUri
        && equalTo(0, (ImmutableRemoteUri) another);
  }

  private boolean equalTo(int synthetic, ImmutableRemoteUri another) {
    return target.equals(another.target)
        && uri.equals(another.uri);
  }

  /**
   * Computes a hash code from attributes: {@code target}, {@code uri}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + target.hashCode();
    h += (h << 5) + uri.hashCode();
    return h;
  }

  /**
   * Construct a new immutable {@code RemoteUri} instance.
   * @param target The value for the {@code target} attribute
   * @param uri The value for the {@code uri} attribute
   * @return An immutable RemoteUri instance
   */
  public static ImmutableRemoteUri of(DeephavenTarget target, StructuredUri uri) {
    return new ImmutableRemoteUri(target, uri);
  }

  /**
   * Creates an immutable copy of a {@link RemoteUri} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable RemoteUri instance
   */
  public static ImmutableRemoteUri copyOf(RemoteUri instance) {
    if (instance instanceof ImmutableRemoteUri) {
      return (ImmutableRemoteUri) instance;
    }
    return ImmutableRemoteUri.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableRemoteUri ImmutableRemoteUri}.
   * <pre>
   * ImmutableRemoteUri.builder()
   *    .target(io.deephaven.uri.DeephavenTarget) // required {@link RemoteUri#target() target}
   *    .uri(io.deephaven.uri.StructuredUri) // required {@link RemoteUri#uri() uri}
   *    .build();
   * </pre>
   * @return A new ImmutableRemoteUri builder
   */
  public static ImmutableRemoteUri.Builder builder() {
    return new ImmutableRemoteUri.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableRemoteUri ImmutableRemoteUri}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "RemoteUri", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_TARGET = 0x1L;
    private static final long INIT_BIT_URI = 0x2L;
    private long initBits = 0x3L;

    private DeephavenTarget target;
    private StructuredUri uri;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code RemoteUri} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(RemoteUri instance) {
      Objects.requireNonNull(instance, "instance");
      target(instance.target());
      uri(instance.uri());
      return this;
    }

    /**
     * Initializes the value for the {@link RemoteUri#target() target} attribute.
     * @param target The value for target 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder target(DeephavenTarget target) {
      this.target = Objects.requireNonNull(target, "target");
      initBits &= ~INIT_BIT_TARGET;
      return this;
    }

    /**
     * Initializes the value for the {@link RemoteUri#uri() uri} attribute.
     * @param uri The value for uri 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder uri(StructuredUri uri) {
      this.uri = Objects.requireNonNull(uri, "uri");
      initBits &= ~INIT_BIT_URI;
      return this;
    }

    /**
     * Builds a new {@link ImmutableRemoteUri ImmutableRemoteUri}.
     * @return An immutable instance of RemoteUri
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableRemoteUri build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableRemoteUri(null, target, uri);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_TARGET) != 0) attributes.add("target");
      if ((initBits & INIT_BIT_URI) != 0) attributes.add("uri");
      return "Cannot build RemoteUri, some of required attributes are not set " + attributes;
    }
  }
}
