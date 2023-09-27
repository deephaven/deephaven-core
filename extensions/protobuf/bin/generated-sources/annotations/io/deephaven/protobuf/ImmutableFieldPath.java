package io.deephaven.protobuf;

import com.google.protobuf.Descriptors;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link FieldPath}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableFieldPath.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableFieldPath.of()}.
 */
@Generated(from = "FieldPath", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableFieldPath extends FieldPath {
  private final List<Descriptors.FieldDescriptor> path;

  private ImmutableFieldPath(Iterable<? extends Descriptors.FieldDescriptor> path) {
    this.path = createUnmodifiableList(false, createSafeList(path, true, false));
  }

  private ImmutableFieldPath(ImmutableFieldPath original, List<Descriptors.FieldDescriptor> path) {
    this.path = path;
  }

  /**
   * The ordered field descriptors which make up the field path.
   * @return the path
   */
  @Override
  public List<Descriptors.FieldDescriptor> path() {
    return path;
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link FieldPath#path() path}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableFieldPath withPath(Descriptors.FieldDescriptor... elements) {
    List<Descriptors.FieldDescriptor> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return new ImmutableFieldPath(this, newValue);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link FieldPath#path() path}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of path elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableFieldPath withPath(Iterable<? extends Descriptors.FieldDescriptor> elements) {
    if (this.path == elements) return this;
    List<Descriptors.FieldDescriptor> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return new ImmutableFieldPath(this, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableFieldPath} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableFieldPath
        && equalTo(0, (ImmutableFieldPath) another);
  }

  private boolean equalTo(int synthetic, ImmutableFieldPath another) {
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
   * Prints the immutable value {@code FieldPath} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "FieldPath{"
        + "path=" + path
        + "}";
  }

  private transient volatile long lazyInitBitmap;

  private static final long NUMBER_PATH_LAZY_INIT_BIT = 0x1L;

  private transient FieldNumberPath numberPath;

  /**
   * {@inheritDoc}
   * <p>
   * Returns a lazily initialized value of the {@link FieldPath#numberPath() numberPath} attribute.
   * Initialized once and only once and stored for subsequent access with proper synchronization.
   * In case of any exception or error thrown by the lazy value initializer,
   * the result will not be memoised (i.e. remembered) and on next call computation
   * will be attempted again.
   * @return A lazily initialized value of the {@code numberPath} attribute
   */
  @Override
  public FieldNumberPath numberPath() {
    if ((lazyInitBitmap & NUMBER_PATH_LAZY_INIT_BIT) == 0) {
      synchronized (this) {
        if ((lazyInitBitmap & NUMBER_PATH_LAZY_INIT_BIT) == 0) {
          this.numberPath = Objects.requireNonNull(super.numberPath(), "numberPath");
          lazyInitBitmap |= NUMBER_PATH_LAZY_INIT_BIT;
        }
      }
    }
    return numberPath;
  }

  private static final long NAME_PATH_LAZY_INIT_BIT = 0x2L;

  private transient List<String> namePath;

  /**
   * {@inheritDoc}
   * <p>
   * Returns a lazily initialized value of the {@link FieldPath#namePath() namePath} attribute.
   * Initialized once and only once and stored for subsequent access with proper synchronization.
   * In case of any exception or error thrown by the lazy value initializer,
   * the result will not be memoised (i.e. remembered) and on next call computation
   * will be attempted again.
   * @return A lazily initialized value of the {@code namePath} attribute
   */
  @Override
  public List<String> namePath() {
    if ((lazyInitBitmap & NAME_PATH_LAZY_INIT_BIT) == 0) {
      synchronized (this) {
        if ((lazyInitBitmap & NAME_PATH_LAZY_INIT_BIT) == 0) {
          this.namePath = Objects.requireNonNull(super.namePath(), "namePath");
          lazyInitBitmap |= NAME_PATH_LAZY_INIT_BIT;
        }
      }
    }
    return namePath;
  }

  /**
   * Construct a new immutable {@code FieldPath} instance.
   * @param path The value for the {@code path} attribute
   * @return An immutable FieldPath instance
   */
  public static ImmutableFieldPath of(List<Descriptors.FieldDescriptor> path) {
    return of((Iterable<? extends Descriptors.FieldDescriptor>) path);
  }

  /**
   * Construct a new immutable {@code FieldPath} instance.
   * @param path The value for the {@code path} attribute
   * @return An immutable FieldPath instance
   */
  public static ImmutableFieldPath of(Iterable<? extends Descriptors.FieldDescriptor> path) {
    return new ImmutableFieldPath(path);
  }

  /**
   * Creates an immutable copy of a {@link FieldPath} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable FieldPath instance
   */
  public static ImmutableFieldPath copyOf(FieldPath instance) {
    if (instance instanceof ImmutableFieldPath) {
      return (ImmutableFieldPath) instance;
    }
    return ImmutableFieldPath.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableFieldPath ImmutableFieldPath}.
   * <pre>
   * ImmutableFieldPath.builder()
   *    .addPath|addAllPath(com.google.protobuf.Descriptors.FieldDescriptor) // {@link FieldPath#path() path} elements
   *    .build();
   * </pre>
   * @return A new ImmutableFieldPath builder
   */
  public static ImmutableFieldPath.Builder builder() {
    return new ImmutableFieldPath.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableFieldPath ImmutableFieldPath}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "FieldPath", generator = "Immutables")
  public static final class Builder {
    private List<Descriptors.FieldDescriptor> path = new ArrayList<Descriptors.FieldDescriptor>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code FieldPath} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(FieldPath instance) {
      Objects.requireNonNull(instance, "instance");
      addAllPath(instance.path());
      return this;
    }

    /**
     * Adds one element to {@link FieldPath#path() path} list.
     * @param element A path element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addPath(Descriptors.FieldDescriptor element) {
      this.path.add(Objects.requireNonNull(element, "path element"));
      return this;
    }

    /**
     * Adds elements to {@link FieldPath#path() path} list.
     * @param elements An array of path elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addPath(Descriptors.FieldDescriptor... elements) {
      for (Descriptors.FieldDescriptor element : elements) {
        this.path.add(Objects.requireNonNull(element, "path element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link FieldPath#path() path} list.
     * @param elements An iterable of path elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder path(Iterable<? extends Descriptors.FieldDescriptor> elements) {
      this.path.clear();
      return addAllPath(elements);
    }

    /**
     * Adds elements to {@link FieldPath#path() path} list.
     * @param elements An iterable of path elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllPath(Iterable<? extends Descriptors.FieldDescriptor> elements) {
      for (Descriptors.FieldDescriptor element : elements) {
        this.path.add(Objects.requireNonNull(element, "path element"));
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableFieldPath ImmutableFieldPath}.
     * @return An immutable instance of FieldPath
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableFieldPath build() {
      return new ImmutableFieldPath(null, createUnmodifiableList(true, path));
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

  private static <T> List<T> createUnmodifiableList(boolean clone, List<T> list) {
    switch(list.size()) {
    case 0: return Collections.emptyList();
    case 1: return Collections.singletonList(list.get(0));
    default:
      if (clone) {
        return Collections.unmodifiableList(new ArrayList<>(list));
      } else {
        if (list instanceof ArrayList<?>) {
          ((ArrayList<?>) list).trimToSize();
        }
        return Collections.unmodifiableList(list);
      }
    }
  }
}
