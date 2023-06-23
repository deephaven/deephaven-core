package io.deephaven.qst;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link LabeledValues}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableLabeledValues.builder()}.
 */
@Generated(from = "LabeledValues", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableLabeledValues<T> extends LabeledValues<T> {
  private final Map<String, LabeledValue<T>> map;

  private ImmutableLabeledValues(Map<String, LabeledValue<T>> map) {
    this.map = map;
  }

  /**
   * @return The value of the {@code map} attribute
   */
  @Override
  Map<String, LabeledValue<T>> map() {
    return map;
  }

  /**
   * Copy the current immutable object by replacing the {@link LabeledValues#map() map} map with the specified map.
   * Nulls are not permitted as keys or values.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param entries The entries to be added to the map map
   * @return A modified copy of {@code this} object
   */
  public final ImmutableLabeledValues<T> withMap(Map<String, ? extends LabeledValue<T>> entries) {
    if (this.map == entries) return this;
    Map<String, LabeledValue<T>> newValue = createUnmodifiableMap(true, false, entries);
    return validate(new ImmutableLabeledValues<>(newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableLabeledValues} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableLabeledValues<?>
        && equalTo(0, (ImmutableLabeledValues<?>) another);
  }

  private boolean equalTo(int synthetic, ImmutableLabeledValues<?> another) {
    return map.equals(another.map);
  }

  /**
   * Computes a hash code from attributes: {@code map}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + map.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code LabeledValues} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "LabeledValues{"
        + "map=" + map
        + "}";
  }

  private static <T> ImmutableLabeledValues<T> validate(ImmutableLabeledValues<T> instance) {
    instance.checkKeys();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link LabeledValues} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param <T> generic parameter T
   * @param instance The instance to copy
   * @return A copied immutable LabeledValues instance
   */
  public static <T> ImmutableLabeledValues<T> copyOf(LabeledValues<T> instance) {
    if (instance instanceof ImmutableLabeledValues<?>) {
      return (ImmutableLabeledValues<T>) instance;
    }
    return ImmutableLabeledValues.<T>builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableLabeledValues ImmutableLabeledValues}.
   * <pre>
   * ImmutableLabeledValues.&amp;lt;T&amp;gt;builder()
   *    .putMap|putAllMap(String =&gt; io.deephaven.qst.LabeledValue&amp;lt;T&amp;gt;) // {@link LabeledValues#map() map} mappings
   *    .build();
   * </pre>
   * @param <T> generic parameter T
   * @return A new ImmutableLabeledValues builder
   */
  public static <T> ImmutableLabeledValues.Builder<T> builder() {
    return new ImmutableLabeledValues.Builder<>();
  }

  /**
   * Builds instances of type {@link ImmutableLabeledValues ImmutableLabeledValues}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "LabeledValues", generator = "Immutables")
  public static final class Builder<T> implements LabeledValues.Builder<T> {
    private Map<String, LabeledValue<T>> map = new LinkedHashMap<String, LabeledValue<T>>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code LabeledValues} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> from(LabeledValues<T> instance) {
      Objects.requireNonNull(instance, "instance");
      putAllMap(instance.map());
      return this;
    }

    /**
     * Put one entry to the {@link LabeledValues#map() map} map.
     * @param key The key in the map map
     * @param value The associated value in the map map
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> putMap(String key, LabeledValue<T> value) {
      this.map.put(
          Objects.requireNonNull(key, "map key"),
          value == null ? Objects.requireNonNull(value, "map value for key: " + key) : value);
      return this;
    }

    /**
     * Put one entry to the {@link LabeledValues#map() map} map. Nulls are not permitted
     * @param entry The key and value entry
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> putMap(Map.Entry<String, ? extends LabeledValue<T>> entry) {
      String k = entry.getKey();
      LabeledValue<T> v = entry.getValue();
      this.map.put(
          Objects.requireNonNull(k, "map key"),
          v == null ? Objects.requireNonNull(v, "map value for key: " + k) : v);
      return this;
    }

    /**
     * Sets or replaces all mappings from the specified map as entries for the {@link LabeledValues#map() map} map. Nulls are not permitted
     * @param entries The entries that will be added to the map map
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> map(Map<String, ? extends LabeledValue<T>> entries) {
      this.map.clear();
      return putAllMap(entries);
    }

    /**
     * Put all mappings from the specified map as entries to {@link LabeledValues#map() map} map. Nulls are not permitted
     * @param entries The entries that will be added to the map map
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> putAllMap(Map<String, ? extends LabeledValue<T>> entries) {
      for (Map.Entry<String, ? extends LabeledValue<T>> e : entries.entrySet()) {
        String k = e.getKey();
        LabeledValue<T> v = e.getValue();
        this.map.put(
            Objects.requireNonNull(k, "map key"),
            v == null ? Objects.requireNonNull(v, "map value for key: " + k) : v);
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableLabeledValues ImmutableLabeledValues}.
     * @return An immutable instance of LabeledValues
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableLabeledValues<T> build() {
      return ImmutableLabeledValues.validate(new ImmutableLabeledValues<>(createUnmodifiableMap(false, false, map)));
    }
  }

  private static <K, V> Map<K, V> createUnmodifiableMap(boolean checkNulls, boolean skipNulls, Map<? extends K, ? extends V> map) {
    switch (map.size()) {
    case 0: return Collections.emptyMap();
    case 1: {
      Map.Entry<? extends K, ? extends V> e = map.entrySet().iterator().next();
      K k = e.getKey();
      V v = e.getValue();
      if (checkNulls) {
        Objects.requireNonNull(k, "key");
        if (v == null) Objects.requireNonNull(v, "value for key: " + k);
      }
      if (skipNulls && (k == null || v == null)) {
        return Collections.emptyMap();
      }
      return Collections.singletonMap(k, v);
    }
    default: {
      Map<K, V> linkedMap = new LinkedHashMap<>(map.size());
      if (skipNulls || checkNulls) {
        for (Map.Entry<? extends K, ? extends V> e : map.entrySet()) {
          K k = e.getKey();
          V v = e.getValue();
          if (skipNulls) {
            if (k == null || v == null) continue;
          } else if (checkNulls) {
            Objects.requireNonNull(k, "key");
            if (v == null) Objects.requireNonNull(v, "value for key: " + k);
          }
          linkedMap.put(k, v);
        }
      } else {
        linkedMap.putAll(map);
      }
      return Collections.unmodifiableMap(linkedMap);
    }
    }
  }
}
