package io.deephaven.plugin.js;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link JsPluginManifestPath}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableJsPluginManifestPath.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableJsPluginManifestPath.of()}.
 */
@Generated(from = "JsPluginManifestPath", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableJsPluginManifestPath extends JsPluginManifestPath {
  private final Path path;

  private ImmutableJsPluginManifestPath(Path path) {
    this.path = Objects.requireNonNull(path, "path");
  }

  private ImmutableJsPluginManifestPath(ImmutableJsPluginManifestPath original, Path path) {
    this.path = path;
  }

  /**
   * The manifest root path directory path.
   * 
   * @return the manifest root directory path
   */
  @Override
  public Path path() {
    return path;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JsPluginManifestPath#path() path} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for path
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJsPluginManifestPath withPath(Path value) {
    if (this.path == value) return this;
    Path newValue = Objects.requireNonNull(value, "path");
    return new ImmutableJsPluginManifestPath(this, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableJsPluginManifestPath} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableJsPluginManifestPath
        && equalTo(0, (ImmutableJsPluginManifestPath) another);
  }

  private boolean equalTo(int synthetic, ImmutableJsPluginManifestPath another) {
    return path.equals(another.path);
  }

  /**
   * Computes a hash code from attributes: {@code path}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + path.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code JsPluginManifestPath} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "JsPluginManifestPath{"
        + "path=" + path
        + "}";
  }

  /**
   * Construct a new immutable {@code JsPluginManifestPath} instance.
   * @param path The value for the {@code path} attribute
   * @return An immutable JsPluginManifestPath instance
   */
  public static ImmutableJsPluginManifestPath of(Path path) {
    return new ImmutableJsPluginManifestPath(path);
  }

  /**
   * Creates an immutable copy of a {@link JsPluginManifestPath} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable JsPluginManifestPath instance
   */
  public static ImmutableJsPluginManifestPath copyOf(JsPluginManifestPath instance) {
    if (instance instanceof ImmutableJsPluginManifestPath) {
      return (ImmutableJsPluginManifestPath) instance;
    }
    return ImmutableJsPluginManifestPath.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableJsPluginManifestPath ImmutableJsPluginManifestPath}.
   * <pre>
   * ImmutableJsPluginManifestPath.builder()
   *    .path(java.nio.file.Path) // required {@link JsPluginManifestPath#path() path}
   *    .build();
   * </pre>
   * @return A new ImmutableJsPluginManifestPath builder
   */
  public static ImmutableJsPluginManifestPath.Builder builder() {
    return new ImmutableJsPluginManifestPath.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableJsPluginManifestPath ImmutableJsPluginManifestPath}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "JsPluginManifestPath", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_PATH = 0x1L;
    private long initBits = 0x1L;

    private Path path;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code JsPluginManifestPath} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(JsPluginManifestPath instance) {
      Objects.requireNonNull(instance, "instance");
      path(instance.path());
      return this;
    }

    /**
     * Initializes the value for the {@link JsPluginManifestPath#path() path} attribute.
     * @param path The value for path 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder path(Path path) {
      this.path = Objects.requireNonNull(path, "path");
      initBits &= ~INIT_BIT_PATH;
      return this;
    }

    /**
     * Builds a new {@link ImmutableJsPluginManifestPath ImmutableJsPluginManifestPath}.
     * @return An immutable instance of JsPluginManifestPath
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableJsPluginManifestPath build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableJsPluginManifestPath(null, path);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_PATH) != 0) attributes.add("path");
      return "Cannot build JsPluginManifestPath, some of required attributes are not set " + attributes;
    }
  }
}
