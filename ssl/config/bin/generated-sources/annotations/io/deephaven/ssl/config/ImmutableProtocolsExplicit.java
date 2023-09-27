package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ProtocolsExplicit}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableProtocolsExplicit.builder()}.
 */
@Generated(from = "ProtocolsExplicit", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableProtocolsExplicit extends ProtocolsExplicit {
  private final List<String> values;

  private ImmutableProtocolsExplicit(List<String> values) {
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
   * Copy the current immutable object with elements that replace the content of {@link ProtocolsExplicit#values() values}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableProtocolsExplicit withValues(String... elements) {
    List<String> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableProtocolsExplicit(newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ProtocolsExplicit#values() values}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of values elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableProtocolsExplicit withValues(Iterable<String> elements) {
    if (this.values == elements) return this;
    List<String> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableProtocolsExplicit(newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableProtocolsExplicit} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableProtocolsExplicit
        && equalTo(0, (ImmutableProtocolsExplicit) another);
  }

  private boolean equalTo(int synthetic, ImmutableProtocolsExplicit another) {
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
   * Prints the immutable value {@code ProtocolsExplicit} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ProtocolsExplicit{"
        + "values=" + values
        + "}";
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "ProtocolsExplicit", generator = "Immutables")
  @Deprecated
  @JsonDeserialize
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends ProtocolsExplicit {
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
  static ImmutableProtocolsExplicit fromJson(Json json) {
    ImmutableProtocolsExplicit.Builder builder = ImmutableProtocolsExplicit.builder();
    if (json.values != null) {
      builder.addAllValues(json.values);
    }
    return builder.build();
  }

  private static ImmutableProtocolsExplicit validate(ImmutableProtocolsExplicit instance) {
    instance.checkNonEmpty();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link ProtocolsExplicit} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ProtocolsExplicit instance
   */
  public static ImmutableProtocolsExplicit copyOf(ProtocolsExplicit instance) {
    if (instance instanceof ImmutableProtocolsExplicit) {
      return (ImmutableProtocolsExplicit) instance;
    }
    return ImmutableProtocolsExplicit.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableProtocolsExplicit ImmutableProtocolsExplicit}.
   * <pre>
   * ImmutableProtocolsExplicit.builder()
   *    .addValues|addAllValues(String) // {@link ProtocolsExplicit#values() values} elements
   *    .build();
   * </pre>
   * @return A new ImmutableProtocolsExplicit builder
   */
  public static ImmutableProtocolsExplicit.Builder builder() {
    return new ImmutableProtocolsExplicit.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableProtocolsExplicit ImmutableProtocolsExplicit}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ProtocolsExplicit", generator = "Immutables")
  public static final class Builder {
    private List<String> values = new ArrayList<String>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ProtocolsExplicit} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(ProtocolsExplicit instance) {
      Objects.requireNonNull(instance, "instance");
      addAllValues(instance.values());
      return this;
    }

    /**
     * Adds one element to {@link ProtocolsExplicit#values() values} list.
     * @param element A values element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addValues(String element) {
      this.values.add(Objects.requireNonNull(element, "values element"));
      return this;
    }

    /**
     * Adds elements to {@link ProtocolsExplicit#values() values} list.
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
     * Sets or replaces all elements for {@link ProtocolsExplicit#values() values} list.
     * @param elements An iterable of values elements
     * @return {@code this} builder for use in a chained invocation
     */
    @JsonProperty("values")
    public final Builder values(Iterable<String> elements) {
      this.values.clear();
      return addAllValues(elements);
    }

    /**
     * Adds elements to {@link ProtocolsExplicit#values() values} list.
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
     * Builds a new {@link ImmutableProtocolsExplicit ImmutableProtocolsExplicit}.
     * @return An immutable instance of ProtocolsExplicit
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableProtocolsExplicit build() {
      return ImmutableProtocolsExplicit.validate(new ImmutableProtocolsExplicit(createUnmodifiableList(true, values)));
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
