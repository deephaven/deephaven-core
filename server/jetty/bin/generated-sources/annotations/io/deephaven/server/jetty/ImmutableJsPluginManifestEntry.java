package io.deephaven.server.jetty;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link JsPluginManifestEntry}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableJsPluginManifestEntry.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableJsPluginManifestEntry.of()}.
 */
@Generated(from = "JsPluginManifestEntry", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
final class ImmutableJsPluginManifestEntry extends JsPluginManifestEntry {
  private final String name;
  private final String version;
  private final String main;

  private ImmutableJsPluginManifestEntry(String name, String version, String main) {
    this.name = Objects.requireNonNull(name, "name");
    this.version = Objects.requireNonNull(version, "version");
    this.main = Objects.requireNonNull(main, "main");
  }

  private ImmutableJsPluginManifestEntry(
      ImmutableJsPluginManifestEntry original,
      String name,
      String version,
      String main) {
    this.name = name;
    this.version = version;
    this.main = main;
  }

  /**
   * @return The value of the {@code name} attribute
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * @return The value of the {@code version} attribute
   */
  @Override
  public String version() {
    return version;
  }

  /**
   * @return The value of the {@code main} attribute
   */
  @Override
  public String main() {
    return main;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JsPluginManifestEntry#name() name} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for name
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJsPluginManifestEntry withName(String value) {
    String newValue = Objects.requireNonNull(value, "name");
    if (this.name.equals(newValue)) return this;
    return new ImmutableJsPluginManifestEntry(this, newValue, this.version, this.main);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JsPluginManifestEntry#version() version} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for version
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJsPluginManifestEntry withVersion(String value) {
    String newValue = Objects.requireNonNull(value, "version");
    if (this.version.equals(newValue)) return this;
    return new ImmutableJsPluginManifestEntry(this, this.name, newValue, this.main);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JsPluginManifestEntry#main() main} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for main
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJsPluginManifestEntry withMain(String value) {
    String newValue = Objects.requireNonNull(value, "main");
    if (this.main.equals(newValue)) return this;
    return new ImmutableJsPluginManifestEntry(this, this.name, this.version, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableJsPluginManifestEntry} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableJsPluginManifestEntry
        && equalTo(0, (ImmutableJsPluginManifestEntry) another);
  }

  private boolean equalTo(int synthetic, ImmutableJsPluginManifestEntry another) {
    return name.equals(another.name)
        && version.equals(another.version)
        && main.equals(another.main);
  }

  /**
   * Computes a hash code from attributes: {@code name}, {@code version}, {@code main}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + name.hashCode();
    h += (h << 5) + version.hashCode();
    h += (h << 5) + main.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code JsPluginManifestEntry} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("JsPluginManifestEntry")
        .omitNullValues()
        .add("name", name)
        .add("version", version)
        .add("main", main)
        .toString();
  }

  /**
   * Construct a new immutable {@code JsPluginManifestEntry} instance.
   * @param name The value for the {@code name} attribute
   * @param version The value for the {@code version} attribute
   * @param main The value for the {@code main} attribute
   * @return An immutable JsPluginManifestEntry instance
   */
  public static ImmutableJsPluginManifestEntry of(String name, String version, String main) {
    return new ImmutableJsPluginManifestEntry(name, version, main);
  }

  /**
   * Creates an immutable copy of a {@link JsPluginManifestEntry} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable JsPluginManifestEntry instance
   */
  public static ImmutableJsPluginManifestEntry copyOf(JsPluginManifestEntry instance) {
    if (instance instanceof ImmutableJsPluginManifestEntry) {
      return (ImmutableJsPluginManifestEntry) instance;
    }
    return ImmutableJsPluginManifestEntry.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableJsPluginManifestEntry ImmutableJsPluginManifestEntry}.
   * <pre>
   * ImmutableJsPluginManifestEntry.builder()
   *    .name(String) // required {@link JsPluginManifestEntry#name() name}
   *    .version(String) // required {@link JsPluginManifestEntry#version() version}
   *    .main(String) // required {@link JsPluginManifestEntry#main() main}
   *    .build();
   * </pre>
   * @return A new ImmutableJsPluginManifestEntry builder
   */
  public static ImmutableJsPluginManifestEntry.Builder builder() {
    return new ImmutableJsPluginManifestEntry.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableJsPluginManifestEntry ImmutableJsPluginManifestEntry}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "JsPluginManifestEntry", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_NAME = 0x1L;
    private static final long INIT_BIT_VERSION = 0x2L;
    private static final long INIT_BIT_MAIN = 0x4L;
    private long initBits = 0x7L;

    private @Nullable String name;
    private @Nullable String version;
    private @Nullable String main;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code JsPluginManifestEntry} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(JsPluginManifestEntry instance) {
      Objects.requireNonNull(instance, "instance");
      name(instance.name());
      version(instance.version());
      main(instance.main());
      return this;
    }

    /**
     * Initializes the value for the {@link JsPluginManifestEntry#name() name} attribute.
     * @param name The value for name 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder name(String name) {
      this.name = Objects.requireNonNull(name, "name");
      initBits &= ~INIT_BIT_NAME;
      return this;
    }

    /**
     * Initializes the value for the {@link JsPluginManifestEntry#version() version} attribute.
     * @param version The value for version 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder version(String version) {
      this.version = Objects.requireNonNull(version, "version");
      initBits &= ~INIT_BIT_VERSION;
      return this;
    }

    /**
     * Initializes the value for the {@link JsPluginManifestEntry#main() main} attribute.
     * @param main The value for main 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder main(String main) {
      this.main = Objects.requireNonNull(main, "main");
      initBits &= ~INIT_BIT_MAIN;
      return this;
    }

    /**
     * Builds a new {@link ImmutableJsPluginManifestEntry ImmutableJsPluginManifestEntry}.
     * @return An immutable instance of JsPluginManifestEntry
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableJsPluginManifestEntry build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableJsPluginManifestEntry(null, name, version, main);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_NAME) != 0) attributes.add("name");
      if ((initBits & INIT_BIT_VERSION) != 0) attributes.add("version");
      if ((initBits & INIT_BIT_MAIN) != 0) attributes.add("main");
      return "Cannot build JsPluginManifestEntry, some of required attributes are not set " + attributes;
    }
  }
}
