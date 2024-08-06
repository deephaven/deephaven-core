package io.deephaven.plugin.js;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link PathsPrefixes}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutablePathsPrefixes.builder()}.
 */
@Generated(from = "PathsPrefixes", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutablePathsPrefixes extends PathsPrefixes {
  private final Set<Path> prefixes;

  private ImmutablePathsPrefixes(Set<Path> prefixes) {
    this.prefixes = prefixes;
  }

  /**
   * @return The value of the {@code prefixes} attribute
   */
  @Override
  public Set<Path> prefixes() {
    return prefixes;
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link PathsPrefixes#prefixes() prefixes}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutablePathsPrefixes withPrefixes(Path... elements) {
    Set<Path> newValue = createUnmodifiableSet(createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutablePathsPrefixes(newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link PathsPrefixes#prefixes() prefixes}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of prefixes elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutablePathsPrefixes withPrefixes(Iterable<? extends Path> elements) {
    if (this.prefixes == elements) return this;
    Set<Path> newValue = createUnmodifiableSet(createSafeList(elements, true, false));
    return validate(new ImmutablePathsPrefixes(newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutablePathsPrefixes} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutablePathsPrefixes
        && equalTo(0, (ImmutablePathsPrefixes) another);
  }

  private boolean equalTo(int synthetic, ImmutablePathsPrefixes another) {
    return prefixes.equals(another.prefixes);
  }

  /**
   * Computes a hash code from attributes: {@code prefixes}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + prefixes.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code PathsPrefixes} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "PathsPrefixes{"
        + "prefixes=" + prefixes
        + "}";
  }

  private static ImmutablePathsPrefixes validate(ImmutablePathsPrefixes instance) {
    instance.checkPrefixesNonEmpty();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link PathsPrefixes} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable PathsPrefixes instance
   */
  public static ImmutablePathsPrefixes copyOf(PathsPrefixes instance) {
    if (instance instanceof ImmutablePathsPrefixes) {
      return (ImmutablePathsPrefixes) instance;
    }
    return ImmutablePathsPrefixes.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutablePathsPrefixes ImmutablePathsPrefixes}.
   * <pre>
   * ImmutablePathsPrefixes.builder()
   *    .addPrefixes|addAllPrefixes(java.nio.file.Path) // {@link PathsPrefixes#prefixes() prefixes} elements
   *    .build();
   * </pre>
   * @return A new ImmutablePathsPrefixes builder
   */
  public static ImmutablePathsPrefixes.Builder builder() {
    return new ImmutablePathsPrefixes.Builder();
  }

  /**
   * Builds instances of type {@link ImmutablePathsPrefixes ImmutablePathsPrefixes}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "PathsPrefixes", generator = "Immutables")
  public static final class Builder implements PathsPrefixes.Builder {
    private List<Path> prefixes = new ArrayList<Path>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code PathsPrefixes} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(PathsPrefixes instance) {
      Objects.requireNonNull(instance, "instance");
      addAllPrefixes(instance.prefixes());
      return this;
    }

    /**
     * Adds one element to {@link PathsPrefixes#prefixes() prefixes} set.
     * @param element A prefixes element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addPrefixes(Path element) {
      this.prefixes.add(Objects.requireNonNull(element, "prefixes element"));
      return this;
    }

    /**
     * Adds elements to {@link PathsPrefixes#prefixes() prefixes} set.
     * @param elements An array of prefixes elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addPrefixes(Path... elements) {
      for (Path element : elements) {
        this.prefixes.add(Objects.requireNonNull(element, "prefixes element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link PathsPrefixes#prefixes() prefixes} set.
     * @param elements An iterable of prefixes elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder prefixes(Iterable<? extends Path> elements) {
      this.prefixes.clear();
      return addAllPrefixes(elements);
    }

    /**
     * Adds elements to {@link PathsPrefixes#prefixes() prefixes} set.
     * @param elements An iterable of prefixes elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllPrefixes(Iterable<? extends Path> elements) {
      for (Path element : elements) {
        this.prefixes.add(Objects.requireNonNull(element, "prefixes element"));
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutablePathsPrefixes ImmutablePathsPrefixes}.
     * @return An immutable instance of PathsPrefixes
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutablePathsPrefixes build() {
      return ImmutablePathsPrefixes.validate(new ImmutablePathsPrefixes(createUnmodifiableSet(prefixes)));
    }
  }

  private static <T> List<T> createSafeList(Iterable<? extends T> iterable, boolean checkNulls, boolean skipNulls) {
    ArrayList<T> list;
    if (iterable instanceof Collection<?>) {
      int size = ((Collection<?>) iterable).size();
      if (size == 0) return Collections.emptyList();
      list = new ArrayList<>();
    } else {
      list = new ArrayList<>();
    }
    for (T element : iterable) {
      if (skipNulls && element == null) continue;
      if (checkNulls) Objects.requireNonNull(element, "element");
      list.add(element);
    }
    return list;
  }

  /** Unmodifiable set constructed from list to avoid rehashing. */
  private static <T> Set<T> createUnmodifiableSet(List<T> list) {
    switch(list.size()) {
    case 0: return Collections.emptySet();
    case 1: return Collections.singleton(list.get(0));
    default:
      Set<T> set = new LinkedHashSet<>(list.size());
      set.addAll(list);
      return Collections.unmodifiableSet(set);
    }
  }
}
