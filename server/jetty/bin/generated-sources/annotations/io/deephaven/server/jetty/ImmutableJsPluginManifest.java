package io.deephaven.server.jetty;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.List;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link JsPluginManifest}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableJsPluginManifest.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableJsPluginManifest.of()}.
 */
@Generated(from = "JsPluginManifest", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
final class ImmutableJsPluginManifest extends JsPluginManifest {
  private final ImmutableList<JsPluginManifestEntry> plugins;

  private ImmutableJsPluginManifest(Iterable<? extends JsPluginManifestEntry> plugins) {
    this.plugins = ImmutableList.copyOf(plugins);
  }

  private ImmutableJsPluginManifest(
      ImmutableJsPluginManifest original,
      ImmutableList<JsPluginManifestEntry> plugins) {
    this.plugins = plugins;
  }

  /**
   * @return The value of the {@code plugins} attribute
   */
  @Override
  public ImmutableList<JsPluginManifestEntry> plugins() {
    return plugins;
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link JsPluginManifest#plugins() plugins}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableJsPluginManifest withPlugins(JsPluginManifestEntry... elements) {
    ImmutableList<JsPluginManifestEntry> newValue = ImmutableList.copyOf(elements);
    return new ImmutableJsPluginManifest(this, newValue);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link JsPluginManifest#plugins() plugins}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of plugins elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableJsPluginManifest withPlugins(Iterable<? extends JsPluginManifestEntry> elements) {
    if (this.plugins == elements) return this;
    ImmutableList<JsPluginManifestEntry> newValue = ImmutableList.copyOf(elements);
    return new ImmutableJsPluginManifest(this, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableJsPluginManifest} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableJsPluginManifest
        && equalTo(0, (ImmutableJsPluginManifest) another);
  }

  private boolean equalTo(int synthetic, ImmutableJsPluginManifest another) {
    return plugins.equals(another.plugins);
  }

  /**
   * Computes a hash code from attributes: {@code plugins}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + plugins.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code JsPluginManifest} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("JsPluginManifest")
        .omitNullValues()
        .add("plugins", plugins)
        .toString();
  }

  /**
   * Construct a new immutable {@code JsPluginManifest} instance.
   * @param plugins The value for the {@code plugins} attribute
   * @return An immutable JsPluginManifest instance
   */
  public static ImmutableJsPluginManifest of(List<JsPluginManifestEntry> plugins) {
    return of((Iterable<? extends JsPluginManifestEntry>) plugins);
  }

  /**
   * Construct a new immutable {@code JsPluginManifest} instance.
   * @param plugins The value for the {@code plugins} attribute
   * @return An immutable JsPluginManifest instance
   */
  public static ImmutableJsPluginManifest of(Iterable<? extends JsPluginManifestEntry> plugins) {
    return new ImmutableJsPluginManifest(plugins);
  }

  /**
   * Creates an immutable copy of a {@link JsPluginManifest} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable JsPluginManifest instance
   */
  public static ImmutableJsPluginManifest copyOf(JsPluginManifest instance) {
    if (instance instanceof ImmutableJsPluginManifest) {
      return (ImmutableJsPluginManifest) instance;
    }
    return ImmutableJsPluginManifest.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableJsPluginManifest ImmutableJsPluginManifest}.
   * <pre>
   * ImmutableJsPluginManifest.builder()
   *    .addPlugins|addAllPlugins(io.deephaven.server.jetty.JsPluginManifestEntry) // {@link JsPluginManifest#plugins() plugins} elements
   *    .build();
   * </pre>
   * @return A new ImmutableJsPluginManifest builder
   */
  public static ImmutableJsPluginManifest.Builder builder() {
    return new ImmutableJsPluginManifest.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableJsPluginManifest ImmutableJsPluginManifest}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "JsPluginManifest", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private ImmutableList.Builder<JsPluginManifestEntry> plugins = ImmutableList.builder();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code JsPluginManifest} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(JsPluginManifest instance) {
      Objects.requireNonNull(instance, "instance");
      addAllPlugins(instance.plugins());
      return this;
    }

    /**
     * Adds one element to {@link JsPluginManifest#plugins() plugins} list.
     * @param element A plugins element
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addPlugins(JsPluginManifestEntry element) {
      this.plugins.add(element);
      return this;
    }

    /**
     * Adds elements to {@link JsPluginManifest#plugins() plugins} list.
     * @param elements An array of plugins elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addPlugins(JsPluginManifestEntry... elements) {
      this.plugins.add(elements);
      return this;
    }


    /**
     * Sets or replaces all elements for {@link JsPluginManifest#plugins() plugins} list.
     * @param elements An iterable of plugins elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder plugins(Iterable<? extends JsPluginManifestEntry> elements) {
      this.plugins = ImmutableList.builder();
      return addAllPlugins(elements);
    }

    /**
     * Adds elements to {@link JsPluginManifest#plugins() plugins} list.
     * @param elements An iterable of plugins elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addAllPlugins(Iterable<? extends JsPluginManifestEntry> elements) {
      this.plugins.addAll(elements);
      return this;
    }

    /**
     * Builds a new {@link ImmutableJsPluginManifest ImmutableJsPluginManifest}.
     * @return An immutable instance of JsPluginManifest
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableJsPluginManifest build() {
      return new ImmutableJsPluginManifest(null, plugins.build());
    }
  }
}
