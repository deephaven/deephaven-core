package io.deephaven.plugin.js;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link JsPluginPackagePath}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableJsPluginPackagePath.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableJsPluginPackagePath.of()}.
 */
@Generated(from = "JsPluginPackagePath", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableJsPluginPackagePath extends JsPluginPackagePath {
  private final Path path;

  private ImmutableJsPluginPackagePath(Path path) {
    this.path = Objects.requireNonNull(path, "path");
  }

  private ImmutableJsPluginPackagePath(ImmutableJsPluginPackagePath original, Path path) {
    this.path = path;
  }

  /**
   * The package root directory path.
   * 
   * @return the package root directory path
   */
  @Override
  public Path path() {
    return path;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JsPluginPackagePath#path() path} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for path
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJsPluginPackagePath withPath(Path value) {
    if (this.path == value) return this;
    Path newValue = Objects.requireNonNull(value, "path");
    return new ImmutableJsPluginPackagePath(this, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableJsPluginPackagePath} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableJsPluginPackagePath
        && equalTo(0, (ImmutableJsPluginPackagePath) another);
  }

  private boolean equalTo(int synthetic, ImmutableJsPluginPackagePath another) {
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
   * Prints the immutable value {@code JsPluginPackagePath} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "JsPluginPackagePath{"
        + "path=" + path
        + "}";
  }

  /**
   * Construct a new immutable {@code JsPluginPackagePath} instance.
   * @param path The value for the {@code path} attribute
   * @return An immutable JsPluginPackagePath instance
   */
  public static ImmutableJsPluginPackagePath of(Path path) {
    return new ImmutableJsPluginPackagePath(path);
  }

  /**
   * Creates an immutable copy of a {@link JsPluginPackagePath} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable JsPluginPackagePath instance
   */
  public static ImmutableJsPluginPackagePath copyOf(JsPluginPackagePath instance) {
    if (instance instanceof ImmutableJsPluginPackagePath) {
      return (ImmutableJsPluginPackagePath) instance;
    }
    return ImmutableJsPluginPackagePath.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableJsPluginPackagePath ImmutableJsPluginPackagePath}.
   * <pre>
   * ImmutableJsPluginPackagePath.builder()
   *    .path(java.nio.file.Path) // required {@link JsPluginPackagePath#path() path}
   *    .build();
   * </pre>
   * @return A new ImmutableJsPluginPackagePath builder
   */
  public static ImmutableJsPluginPackagePath.Builder builder() {
    return new ImmutableJsPluginPackagePath.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableJsPluginPackagePath ImmutableJsPluginPackagePath}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "JsPluginPackagePath", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_PATH = 0x1L;
    private long initBits = 0x1L;

    private Path path;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code JsPluginPackagePath} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(JsPluginPackagePath instance) {
      Objects.requireNonNull(instance, "instance");
      path(instance.path());
      return this;
    }

    /**
     * Initializes the value for the {@link JsPluginPackagePath#path() path} attribute.
     * @param path The value for path 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder path(Path path) {
      this.path = Objects.requireNonNull(path, "path");
      initBits &= ~INIT_BIT_PATH;
      return this;
    }

    /**
     * Builds a new {@link ImmutableJsPluginPackagePath ImmutableJsPluginPackagePath}.
     * @return An immutable instance of JsPluginPackagePath
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableJsPluginPackagePath build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableJsPluginPackagePath(null, path);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_PATH) != 0) attributes.add("path");
      return "Cannot build JsPluginPackagePath, some of required attributes are not set " + attributes;
    }
  }
}
