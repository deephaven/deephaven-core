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
 * Immutable implementation of {@link CiphersExplicit}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableCiphersExplicit.builder()}.
 */
@Generated(from = "CiphersExplicit", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableCiphersExplicit extends CiphersExplicit {
  private final List<String> values;

  private ImmutableCiphersExplicit(List<String> values) {
    this.values = values;
  }

  /**
   * @return The value of the {@code values} attribute
   */
  @JsonProperty("values")
  @Override
  public List<String> values() {
    return values;
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link CiphersExplicit#values() values}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableCiphersExplicit withValues(String... elements) {
    List<String> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableCiphersExplicit(newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link CiphersExplicit#values() values}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of values elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableCiphersExplicit withValues(Iterable<String> elements) {
    if (this.values == elements) return this;
    List<String> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableCiphersExplicit(newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableCiphersExplicit} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableCiphersExplicit
        && equalTo(0, (ImmutableCiphersExplicit) another);
  }

  private boolean equalTo(int synthetic, ImmutableCiphersExplicit another) {
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
   * Prints the immutable value {@code CiphersExplicit} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "CiphersExplicit{"
        + "values=" + values
        + "}";
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "CiphersExplicit", generator = "Immutables")
  @Deprecated
  @JsonDeserialize
  @JsonTypeInfo(use=JsonTypeInfo.Id.NONE)
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends CiphersExplicit {
    List<String> values = Collections.emptyList();
    @JsonProperty("values")
    public void setValues(List<String> values) {
      this.values = values;
    }
    @Override
    public List<String> values() { throw new UnsupportedOperationException(); }
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static ImmutableCiphersExplicit fromJson(Json json) {
    ImmutableCiphersExplicit.Builder builder = ImmutableCiphersExplicit.builder();
    if (json.values != null) {
      builder.addAllValues(json.values);
    }
    return builder.build();
  }

  private static ImmutableCiphersExplicit validate(ImmutableCiphersExplicit instance) {
    instance.checkNonEmpty();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link CiphersExplicit} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable CiphersExplicit instance
   */
  public static ImmutableCiphersExplicit copyOf(CiphersExplicit instance) {
    if (instance instanceof ImmutableCiphersExplicit) {
      return (ImmutableCiphersExplicit) instance;
    }
    return ImmutableCiphersExplicit.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableCiphersExplicit ImmutableCiphersExplicit}.
   * <pre>
   * ImmutableCiphersExplicit.builder()
   *    .addValues|addAllValues(String) // {@link CiphersExplicit#values() values} elements
   *    .build();
   * </pre>
   * @return A new ImmutableCiphersExplicit builder
   */
  public static ImmutableCiphersExplicit.Builder builder() {
    return new ImmutableCiphersExplicit.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableCiphersExplicit ImmutableCiphersExplicit}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "CiphersExplicit", generator = "Immutables")
  public static final class Builder {
    private List<String> values = new ArrayList<String>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code CiphersExplicit} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(CiphersExplicit instance) {
      Objects.requireNonNull(instance, "instance");
      addAllValues(instance.values());
      return this;
    }

    /**
     * Adds one element to {@link CiphersExplicit#values() values} list.
     * @param element A values element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addValues(String element) {
      this.values.add(Objects.requireNonNull(element, "values element"));
      return this;
    }

    /**
     * Adds elements to {@link CiphersExplicit#values() values} list.
     * @param elements An array of values elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addValues(String... elements) {
      for (String element : elements) {
        this.values.add(Objects.requireNonNull(element, "values element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link CiphersExplicit#values() values} list.
     * @param elements An iterable of values elements
     * @return {@code this} builder for use in a chained invocation
     */
    @JsonProperty("values")
    public final Builder values(Iterable<String> elements) {
      this.values.clear();
      return addAllValues(elements);
    }

    /**
     * Adds elements to {@link CiphersExplicit#values() values} list.
     * @param elements An iterable of values elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllValues(Iterable<String> elements) {
      for (String element : elements) {
        this.values.add(Objects.requireNonNull(element, "values element"));
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableCiphersExplicit ImmutableCiphersExplicit}.
     * @return An immutable instance of CiphersExplicit
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableCiphersExplicit build() {
      return ImmutableCiphersExplicit.validate(new ImmutableCiphersExplicit(createUnmodifiableList(true, values)));
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
