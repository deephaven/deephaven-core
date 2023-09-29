package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link IdentityList}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableIdentityList.builder()}.
 */
@Generated(from = "IdentityList", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableIdentityList extends IdentityList {
  private final List<Identity> values;

  private ImmutableIdentityList(List<Identity> values) {
    this.values = values;
  }

  /**
   * @return The value of the {@code values} attribute
   */
  @JsonProperty("values")
  @Override
  public List<Identity> values() {
    return values;
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link IdentityList#values() values}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableIdentityList withValues(Identity... elements) {
    List<Identity> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return new ImmutableIdentityList(newValue);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link IdentityList#values() values}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of values elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableIdentityList withValues(Iterable<? extends Identity> elements) {
    if (this.values == elements) return this;
    List<Identity> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return new ImmutableIdentityList(newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableIdentityList} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableIdentityList
        && equalTo(0, (ImmutableIdentityList) another);
  }

  private boolean equalTo(int synthetic, ImmutableIdentityList another) {
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
   * Prints the immutable value {@code IdentityList} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "IdentityList{"
        + "values=" + values
        + "}";
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "IdentityList", generator = "Immutables")
  @Deprecated
  @JsonDeserialize
  @JsonTypeInfo(use=JsonTypeInfo.Id.NONE)
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends IdentityList {
    List<Identity> values = Collections.emptyList();
    @JsonProperty("values")
    public void setValues(List<Identity> values) {
      this.values = values;
    }
    @Override
    public List<Identity> values() { throw new UnsupportedOperationException(); }
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static ImmutableIdentityList fromJson(Json json) {
    ImmutableIdentityList.Builder builder = ImmutableIdentityList.builder();
    if (json.values != null) {
      builder.addAllValues(json.values);
    }
    return builder.build();
  }

  /**
   * Creates an immutable copy of a {@link IdentityList} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable IdentityList instance
   */
  public static ImmutableIdentityList copyOf(IdentityList instance) {
    if (instance instanceof ImmutableIdentityList) {
      return (ImmutableIdentityList) instance;
    }
    return ImmutableIdentityList.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableIdentityList ImmutableIdentityList}.
   * <pre>
   * ImmutableIdentityList.builder()
   *    .addValues|addAllValues(io.deephaven.ssl.config.Identity) // {@link IdentityList#values() values} elements
   *    .build();
   * </pre>
   * @return A new ImmutableIdentityList builder
   */
  public static ImmutableIdentityList.Builder builder() {
    return new ImmutableIdentityList.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableIdentityList ImmutableIdentityList}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "IdentityList", generator = "Immutables")
  public static final class Builder {
    private List<Identity> values = new ArrayList<Identity>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code IdentityList} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(IdentityList instance) {
      Objects.requireNonNull(instance, "instance");
      addAllValues(instance.values());
      return this;
    }

    /**
     * Adds one element to {@link IdentityList#values() values} list.
     * @param element A values element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addValues(Identity element) {
      this.values.add(Objects.requireNonNull(element, "values element"));
      return this;
    }

    /**
     * Adds elements to {@link IdentityList#values() values} list.
     * @param elements An array of values elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addValues(Identity... elements) {
      for (Identity element : elements) {
        this.values.add(Objects.requireNonNull(element, "values element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link IdentityList#values() values} list.
     * @param elements An iterable of values elements
     * @return {@code this} builder for use in a chained invocation
     */
    @JsonProperty("values")
    public final Builder values(Iterable<? extends Identity> elements) {
      this.values.clear();
      return addAllValues(elements);
    }

    /**
     * Adds elements to {@link IdentityList#values() values} list.
     * @param elements An iterable of values elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllValues(Iterable<? extends Identity> elements) {
      for (Identity element : elements) {
        this.values.add(Objects.requireNonNull(element, "values element"));
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableIdentityList ImmutableIdentityList}.
     * @return An immutable instance of IdentityList
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableIdentityList build() {
      return new ImmutableIdentityList(createUnmodifiableList(true, values));
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
