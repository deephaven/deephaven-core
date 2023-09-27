package io.deephaven.protobuf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ProtobufFunctions}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableProtobufFunctions.builder()}.
 */
@Generated(from = "ProtobufFunctions", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableProtobufFunctions extends ProtobufFunctions {
  private final List<ProtobufFunction> functions;

  private ImmutableProtobufFunctions(List<ProtobufFunction> functions) {
    this.functions = functions;
  }

  /**
   * The protobuf functions.
   * @return the functions
   */
  @Override
  public List<ProtobufFunction> functions() {
    return functions;
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ProtobufFunctions#functions() functions}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableProtobufFunctions withFunctions(ProtobufFunction... elements) {
    List<ProtobufFunction> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return new ImmutableProtobufFunctions(newValue);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ProtobufFunctions#functions() functions}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of functions elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableProtobufFunctions withFunctions(Iterable<? extends ProtobufFunction> elements) {
    if (this.functions == elements) return this;
    List<ProtobufFunction> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return new ImmutableProtobufFunctions(newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableProtobufFunctions} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableProtobufFunctions
        && equalTo(0, (ImmutableProtobufFunctions) another);
  }

  private boolean equalTo(int synthetic, ImmutableProtobufFunctions another) {
    return functions.equals(another.functions);
  }

  /**
   * Computes a hash code from attributes: {@code functions}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + functions.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code ProtobufFunctions} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ProtobufFunctions{"
        + "functions=" + functions
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link ProtobufFunctions} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ProtobufFunctions instance
   */
  public static ImmutableProtobufFunctions copyOf(ProtobufFunctions instance) {
    if (instance instanceof ImmutableProtobufFunctions) {
      return (ImmutableProtobufFunctions) instance;
    }
    return ImmutableProtobufFunctions.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableProtobufFunctions ImmutableProtobufFunctions}.
   * <pre>
   * ImmutableProtobufFunctions.builder()
   *    .addFunctions|addAllFunctions(io.deephaven.protobuf.ProtobufFunction) // {@link ProtobufFunctions#functions() functions} elements
   *    .build();
   * </pre>
   * @return A new ImmutableProtobufFunctions builder
   */
  public static ImmutableProtobufFunctions.Builder builder() {
    return new ImmutableProtobufFunctions.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableProtobufFunctions ImmutableProtobufFunctions}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ProtobufFunctions", generator = "Immutables")
  public static final class Builder implements ProtobufFunctions.Builder {
    private List<ProtobufFunction> functions = new ArrayList<ProtobufFunction>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ProtobufFunctions} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(ProtobufFunctions instance) {
      Objects.requireNonNull(instance, "instance");
      addAllFunctions(instance.functions());
      return this;
    }

    /**
     * Adds one element to {@link ProtobufFunctions#functions() functions} list.
     * @param element A functions element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addFunctions(ProtobufFunction element) {
      this.functions.add(Objects.requireNonNull(element, "functions element"));
      return this;
    }

    /**
     * Adds elements to {@link ProtobufFunctions#functions() functions} list.
     * @param elements An array of functions elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addFunctions(ProtobufFunction... elements) {
      for (ProtobufFunction element : elements) {
        this.functions.add(Objects.requireNonNull(element, "functions element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link ProtobufFunctions#functions() functions} list.
     * @param elements An iterable of functions elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder functions(Iterable<? extends ProtobufFunction> elements) {
      this.functions.clear();
      return addAllFunctions(elements);
    }

    /**
     * Adds elements to {@link ProtobufFunctions#functions() functions} list.
     * @param elements An iterable of functions elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllFunctions(Iterable<? extends ProtobufFunction> elements) {
      for (ProtobufFunction element : elements) {
        this.functions.add(Objects.requireNonNull(element, "functions element"));
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableProtobufFunctions ImmutableProtobufFunctions}.
     * @return An immutable instance of ProtobufFunctions
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableProtobufFunctions build() {
      return new ImmutableProtobufFunctions(createUnmodifiableList(true, functions));
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
