package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
<<<<<<< HEAD
import com.fasterxml.jackson.annotation.JsonTypeInfo;
=======
>>>>>>> main
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link TrustList}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableTrustList.builder()}.
 */
@Generated(from = "TrustList", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableTrustList extends TrustList {
  private final List<Trust> values;

  private ImmutableTrustList(List<Trust> values) {
    this.values = values;
  }

  /**
   * @return The value of the {@code values} attribute
   */
  @JsonProperty("values")
  @Override
  public List<Trust> values() {
    return values;
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link TrustList#values() values}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableTrustList withValues(Trust... elements) {
    List<Trust> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableTrustList(newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link TrustList#values() values}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of values elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableTrustList withValues(Iterable<? extends Trust> elements) {
    if (this.values == elements) return this;
    List<Trust> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableTrustList(newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableTrustList} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableTrustList
        && equalTo(0, (ImmutableTrustList) another);
  }

  private boolean equalTo(int synthetic, ImmutableTrustList another) {
    return values.equals(another.values);
  }

  /**
   * Computes a hash code from attributes: {@code values}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + values.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code TrustList} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "TrustList{"
        + "values=" + values
        + "}";
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "TrustList", generator = "Immutables")
  @Deprecated
  @JsonDeserialize
<<<<<<< HEAD
  @JsonTypeInfo(use=JsonTypeInfo.Id.NONE)
=======
>>>>>>> main
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends TrustList {
    List<Trust> values = Collections.emptyList();
    @JsonProperty("values")
    public void setValues(List<Trust> values) {
      this.values = values;
    }
    @Override
    public List<Trust> values() { throw new UnsupportedOperationException(); }
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static ImmutableTrustList fromJson(Json json) {
    ImmutableTrustList.Builder builder = ImmutableTrustList.builder();
    if (json.values != null) {
      builder.addAllValues(json.values);
    }
    return builder.build();
  }

  private static ImmutableTrustList validate(ImmutableTrustList instance) {
    instance.checkNonEmpty();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link TrustList} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable TrustList instance
   */
  public static ImmutableTrustList copyOf(TrustList instance) {
    if (instance instanceof ImmutableTrustList) {
      return (ImmutableTrustList) instance;
    }
    return ImmutableTrustList.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableTrustList ImmutableTrustList}.
   * <pre>
   * ImmutableTrustList.builder()
<<<<<<< HEAD
   *    .addValues|addAllValues(io.deephaven.ssl.config.Trust) // {@link TrustList#values() values} elements
=======
   *    .addValues|addAllValues(Trust) // {@link TrustList#values() values} elements
>>>>>>> main
   *    .build();
   * </pre>
   * @return A new ImmutableTrustList builder
   */
  public static ImmutableTrustList.Builder builder() {
    return new ImmutableTrustList.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableTrustList ImmutableTrustList}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "TrustList", generator = "Immutables")
  public static final class Builder {
    private List<Trust> values = new ArrayList<Trust>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code TrustList} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(TrustList instance) {
      Objects.requireNonNull(instance, "instance");
      addAllValues(instance.values());
      return this;
    }

    /**
     * Adds one element to {@link TrustList#values() values} list.
     * @param element A values element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addValues(Trust element) {
      this.values.add(Objects.requireNonNull(element, "values element"));
      return this;
    }

    /**
     * Adds elements to {@link TrustList#values() values} list.
     * @param elements An array of values elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addValues(Trust... elements) {
      for (Trust element : elements) {
        this.values.add(Objects.requireNonNull(element, "values element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link TrustList#values() values} list.
     * @param elements An iterable of values elements
     * @return {@code this} builder for use in a chained invocation
     */
    @JsonProperty("values")
    public final Builder values(Iterable<? extends Trust> elements) {
      this.values.clear();
      return addAllValues(elements);
    }

    /**
     * Adds elements to {@link TrustList#values() values} list.
     * @param elements An iterable of values elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllValues(Iterable<? extends Trust> elements) {
      for (Trust element : elements) {
        this.values.add(Objects.requireNonNull(element, "values element"));
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableTrustList ImmutableTrustList}.
     * @return An immutable instance of TrustList
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableTrustList build() {
      return ImmutableTrustList.validate(new ImmutableTrustList(createUnmodifiableList(true, values)));
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
