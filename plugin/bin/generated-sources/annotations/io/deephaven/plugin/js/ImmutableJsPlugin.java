package io.deephaven.plugin.js;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link JsPlugin}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableJsPlugin.builder()}.
 */
@Generated(from = "JsPlugin", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableJsPlugin extends JsPlugin {
  private final String name;
  private final String version;
  private final Path main;
  private final Path path;
  private final Paths paths;

  private ImmutableJsPlugin(ImmutableJsPlugin.Builder builder) {
    this.name = builder.name;
    this.version = builder.version;
    this.main = builder.main;
    this.path = builder.path;
    this.paths = builder.paths != null
        ? builder.paths
        : Objects.requireNonNull(super.paths(), "paths");
  }

  private ImmutableJsPlugin(
      String name,
      String version,
      Path main,
      Path path,
      Paths paths) {
    this.name = name;
    this.version = version;
    this.main = main;
    this.path = path;
    this.paths = paths;
  }

  /**
   * The JS plugin name. The JS plugin contents will be served under the URL path "js-plugins/{name}/", as well as
   * included as the "name" field for the manifest entry in "js-plugins/manifest.json". The "/" character will not be
   * URL encoded - the name "@example/foo" would be served under the URL path "js-plugins/@example/foo/".
   * @return the name
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * The JS plugin version. Will be included as the "version" field for the manifest entry in
   * "js-plugins/manifest.json".
   * @return the version
   */
  @Override
  public String version() {
    return version;
  }

  /**
   * The main JS file path, specified relative to {@link #path()}. The main JS file must exist
   * ({@code Files.isRegularFile(root().resolve(main()))}) and must be included in {@link #paths()}. Will be included
   * as the "main" field for the manifest entry in "js-plugins/manifest.json".
   * @return the main JS file path
   */
  @Override
  public Path main() {
    return main;
  }

  /**
   * The directory path of the resources to serve. The resources will be served via the URL path
   * "js-plugins/{name}/{relativeToPath}". The path must exist ({@code Files.isDirectory(path())}).
   * @return the path
   */
  @Override
  public Path path() {
    return path;
  }

  /**
   * The subset of resources from {@link #path()} to serve. Production installations should preferably be packaged
   * with the exact resources necessary (and thus served with {@link Paths#all()}). During development, other subsets
   * may be useful if {@link #path()} contains content unrelated to the JS content. By default, is
   * {@link Paths#all()}.
   * @return the paths
   */
  @Override
  public Paths paths() {
    return paths;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JsPlugin#name() name} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for name
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJsPlugin withName(String value) {
    String newValue = Objects.requireNonNull(value, "name");
    if (this.name.equals(newValue)) return this;
    return validate(new ImmutableJsPlugin(newValue, this.version, this.main, this.path, this.paths));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JsPlugin#version() version} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for version
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJsPlugin withVersion(String value) {
    String newValue = Objects.requireNonNull(value, "version");
    if (this.version.equals(newValue)) return this;
    return validate(new ImmutableJsPlugin(this.name, newValue, this.main, this.path, this.paths));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JsPlugin#main() main} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for main
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJsPlugin withMain(Path value) {
    if (this.main == value) return this;
    Path newValue = Objects.requireNonNull(value, "main");
    return validate(new ImmutableJsPlugin(this.name, this.version, newValue, this.path, this.paths));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JsPlugin#path() path} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for path
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJsPlugin withPath(Path value) {
    if (this.path == value) return this;
    Path newValue = Objects.requireNonNull(value, "path");
    return validate(new ImmutableJsPlugin(this.name, this.version, this.main, newValue, this.paths));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JsPlugin#paths() paths} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for paths
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJsPlugin withPaths(Paths value) {
    if (this.paths == value) return this;
    Paths newValue = Objects.requireNonNull(value, "paths");
    return validate(new ImmutableJsPlugin(this.name, this.version, this.main, this.path, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableJsPlugin} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableJsPlugin
        && equalTo(0, (ImmutableJsPlugin) another);
  }

  private boolean equalTo(int synthetic, ImmutableJsPlugin another) {
    return name.equals(another.name)
        && version.equals(another.version)
        && main.equals(another.main)
        && path.equals(another.path)
        && paths.equals(another.paths);
  }

  /**
   * Computes a hash code from attributes: {@code name}, {@code version}, {@code main}, {@code path}, {@code paths}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + name.hashCode();
    h += (h << 5) + version.hashCode();
    h += (h << 5) + main.hashCode();
    h += (h << 5) + path.hashCode();
    h += (h << 5) + paths.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code JsPlugin} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "JsPlugin{"
        + "name=" + name
        + ", version=" + version
        + ", main=" + main
        + ", path=" + path
        + ", paths=" + paths
        + "}";
  }

  private static ImmutableJsPlugin validate(ImmutableJsPlugin instance) {
    instance.checkPaths();
    instance.checkMain();
    instance.checkPath();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link JsPlugin} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable JsPlugin instance
   */
  public static ImmutableJsPlugin copyOf(JsPlugin instance) {
    if (instance instanceof ImmutableJsPlugin) {
      return (ImmutableJsPlugin) instance;
    }
    return ImmutableJsPlugin.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableJsPlugin ImmutableJsPlugin}.
   * <pre>
   * ImmutableJsPlugin.builder()
   *    .name(String) // required {@link JsPlugin#name() name}
   *    .version(String) // required {@link JsPlugin#version() version}
   *    .main(java.nio.file.Path) // required {@link JsPlugin#main() main}
   *    .path(java.nio.file.Path) // required {@link JsPlugin#path() path}
   *    .paths(io.deephaven.plugin.js.Paths) // optional {@link JsPlugin#paths() paths}
   *    .build();
   * </pre>
   * @return A new ImmutableJsPlugin builder
   */
  public static ImmutableJsPlugin.Builder builder() {
    return new ImmutableJsPlugin.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableJsPlugin ImmutableJsPlugin}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "JsPlugin", generator = "Immutables")
  public static final class Builder implements JsPlugin.Builder {
    private static final long INIT_BIT_NAME = 0x1L;
    private static final long INIT_BIT_VERSION = 0x2L;
    private static final long INIT_BIT_MAIN = 0x4L;
    private static final long INIT_BIT_PATH = 0x8L;
    private long initBits = 0xfL;

    private String name;
    private String version;
    private Path main;
    private Path path;
    private Paths paths;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code JsPlugin} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(JsPlugin instance) {
      Objects.requireNonNull(instance, "instance");
      name(instance.name());
      version(instance.version());
      main(instance.main());
      path(instance.path());
      paths(instance.paths());
      return this;
    }

    /**
     * Initializes the value for the {@link JsPlugin#name() name} attribute.
     * @param name The value for name 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder name(String name) {
      this.name = Objects.requireNonNull(name, "name");
      initBits &= ~INIT_BIT_NAME;
      return this;
    }

    /**
     * Initializes the value for the {@link JsPlugin#version() version} attribute.
     * @param version The value for version 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder version(String version) {
      this.version = Objects.requireNonNull(version, "version");
      initBits &= ~INIT_BIT_VERSION;
      return this;
    }

    /**
     * Initializes the value for the {@link JsPlugin#main() main} attribute.
     * @param main The value for main 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder main(Path main) {
      this.main = Objects.requireNonNull(main, "main");
      initBits &= ~INIT_BIT_MAIN;
      return this;
    }

    /**
     * Initializes the value for the {@link JsPlugin#path() path} attribute.
     * @param path The value for path 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder path(Path path) {
      this.path = Objects.requireNonNull(path, "path");
      initBits &= ~INIT_BIT_PATH;
      return this;
    }

    /**
     * Initializes the value for the {@link JsPlugin#paths() paths} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link JsPlugin#paths() paths}.</em>
     * @param paths The value for paths 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder paths(Paths paths) {
      this.paths = Objects.requireNonNull(paths, "paths");
      return this;
    }

    /**
     * Builds a new {@link ImmutableJsPlugin ImmutableJsPlugin}.
     * @return An immutable instance of JsPlugin
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableJsPlugin build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableJsPlugin.validate(new ImmutableJsPlugin(this));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_NAME) != 0) attributes.add("name");
      if ((initBits & INIT_BIT_VERSION) != 0) attributes.add("version");
      if ((initBits & INIT_BIT_MAIN) != 0) attributes.add("main");
      if ((initBits & INIT_BIT_PATH) != 0) attributes.add("path");
      return "Cannot build JsPlugin, some of required attributes are not set " + attributes;
    }
  }
}
