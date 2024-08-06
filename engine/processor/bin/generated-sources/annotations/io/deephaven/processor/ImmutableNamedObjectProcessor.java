package io.deephaven.processor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link NamedObjectProcessor}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableNamedObjectProcessor.builder()}.
 */
@Generated(from = "NamedObjectProcessor", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableNamedObjectProcessor<T> extends NamedObjectProcessor<T> {
  private final List<String> names;
  private final ObjectProcessor<? super T> processor;

  private ImmutableNamedObjectProcessor(List<String> names, ObjectProcessor<? super T> processor) {
    this.names = names;
    this.processor = processor;
  }

  /**
   * The name for each output of {@link #processor()}.
   */
  @Override
  public List<String> names() {
    return names;
  }

  /**
   * The object processor.
   */
  @Override
  public ObjectProcessor<? super T> processor() {
    return processor;
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link NamedObjectProcessor#names() names}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableNamedObjectProcessor<T> withNames(String... elements) {
    List<String> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableNamedObjectProcessor<>(newValue, this.processor));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link NamedObjectProcessor#names() names}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of names elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableNamedObjectProcessor<T> withNames(Iterable<String> elements) {
    if (this.names == elements) return this;
    List<String> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableNamedObjectProcessor<>(newValue, this.processor));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link NamedObjectProcessor#processor() processor} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for processor
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableNamedObjectProcessor<T> withProcessor(ObjectProcessor<? super T> value) {
    if (this.processor == value) return this;
    ObjectProcessor<? super T> newValue = Objects.requireNonNull(value, "processor");
    return validate(new ImmutableNamedObjectProcessor<>(this.names, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableNamedObjectProcessor} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableNamedObjectProcessor<?>
        && equalTo(0, (ImmutableNamedObjectProcessor<?>) another);
  }

  private boolean equalTo(int synthetic, ImmutableNamedObjectProcessor<?> another) {
    return names.equals(another.names)
        && processor.equals(another.processor);
  }

  /**
   * Computes a hash code from attributes: {@code names}, {@code processor}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + names.hashCode();
    h += (h << 5) + processor.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code NamedObjectProcessor} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "NamedObjectProcessor{"
        + "names=" + names
        + ", processor=" + processor
        + "}";
  }

  private static <T> ImmutableNamedObjectProcessor<T> validate(ImmutableNamedObjectProcessor<T> instance) {
    instance.checkSizes();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link NamedObjectProcessor} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param <T> generic parameter T
   * @param instance The instance to copy
   * @return A copied immutable NamedObjectProcessor instance
   */
  public static <T> ImmutableNamedObjectProcessor<T> copyOf(NamedObjectProcessor<T> instance) {
    if (instance instanceof ImmutableNamedObjectProcessor<?>) {
      return (ImmutableNamedObjectProcessor<T>) instance;
    }
    return ImmutableNamedObjectProcessor.<T>builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableNamedObjectProcessor ImmutableNamedObjectProcessor}.
   * <pre>
   * ImmutableNamedObjectProcessor.&amp;lt;T&amp;gt;builder()
   *    .addNames|addAllNames(String) // {@link NamedObjectProcessor#names() names} elements
   *    .processor(io.deephaven.processor.ObjectProcessor&amp;lt;? super T&amp;gt;) // required {@link NamedObjectProcessor#processor() processor}
   *    .build();
   * </pre>
   * @param <T> generic parameter T
   * @return A new ImmutableNamedObjectProcessor builder
   */
  public static <T> ImmutableNamedObjectProcessor.Builder<T> builder() {
    return new ImmutableNamedObjectProcessor.Builder<>();
  }

  /**
   * Builds instances of type {@link ImmutableNamedObjectProcessor ImmutableNamedObjectProcessor}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "NamedObjectProcessor", generator = "Immutables")
  public static final class Builder<T> implements NamedObjectProcessor.Builder<T> {
    private static final long INIT_BIT_PROCESSOR = 0x1L;
    private long initBits = 0x1L;

    private List<String> names = new ArrayList<String>();
    private ObjectProcessor<? super T> processor;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code NamedObjectProcessor} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> from(NamedObjectProcessor<T> instance) {
      Objects.requireNonNull(instance, "instance");
      addAllNames(instance.names());
      processor(instance.processor());
      return this;
    }

    /**
     * Adds one element to {@link NamedObjectProcessor#names() names} list.
     * @param element A names element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> addNames(String element) {
      this.names.add(Objects.requireNonNull(element, "names element"));
      return this;
    }

    /**
     * Adds elements to {@link NamedObjectProcessor#names() names} list.
     * @param elements An array of names elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> addNames(String... elements) {
      for (String element : elements) {
        this.names.add(Objects.requireNonNull(element, "names element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link NamedObjectProcessor#names() names} list.
     * @param elements An iterable of names elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> names(Iterable<String> elements) {
      this.names.clear();
      return addAllNames(elements);
    }

    /**
     * Adds elements to {@link NamedObjectProcessor#names() names} list.
     * @param elements An iterable of names elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> addAllNames(Iterable<String> elements) {
      for (String element : elements) {
        this.names.add(Objects.requireNonNull(element, "names element"));
      }
      return this;
    }

    /**
     * Initializes the value for the {@link NamedObjectProcessor#processor() processor} attribute.
     * @param processor The value for processor 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> processor(ObjectProcessor<? super T> processor) {
      this.processor = Objects.requireNonNull(processor, "processor");
      initBits &= ~INIT_BIT_PROCESSOR;
      return this;
    }

    /**
     * Builds a new {@link ImmutableNamedObjectProcessor ImmutableNamedObjectProcessor}.
     * @return An immutable instance of NamedObjectProcessor
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableNamedObjectProcessor<T> build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableNamedObjectProcessor.validate(new ImmutableNamedObjectProcessor<>(createUnmodifiableList(true, names), processor));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_PROCESSOR) != 0) attributes.add("processor");
      return "Cannot build NamedObjectProcessor, some of required attributes are not set " + attributes;
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
