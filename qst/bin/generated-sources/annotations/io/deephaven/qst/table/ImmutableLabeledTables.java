package io.deephaven.qst.table;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link LabeledTables}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableLabeledTables.builder()}.
 */
@Generated(from = "LabeledTables", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableLabeledTables extends LabeledTables {
  private final Map<String, TableSpec> map;

  private ImmutableLabeledTables(Map<String, TableSpec> map) {
    this.map = map;
  }

  /**
   * @return The value of the {@code map} attribute
   */
  @Override
  Map<String, TableSpec> map() {
    return map;
  }

  /**
   * Copy the current immutable object by replacing the {@link LabeledTables#map() map} map with the specified map.
   * Nulls are not permitted as keys or values.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param entries The entries to be added to the map map
   * @return A modified copy of {@code this} object
   */
  public final ImmutableLabeledTables withMap(Map<String, ? extends TableSpec> entries) {
    if (this.map == entries) return this;
    Map<String, TableSpec> newValue = createUnmodifiableMap(true, false, entries);
    return new ImmutableLabeledTables(newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableLabeledTables} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableLabeledTables
        && equalTo(0, (ImmutableLabeledTables) another);
  }

  private boolean equalTo(int synthetic, ImmutableLabeledTables another) {
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
   * Prints the immutable value {@code LabeledTables} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "LabeledTables{"
        + "map=" + map
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link LabeledTables} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable LabeledTables instance
   */
  public static ImmutableLabeledTables copyOf(LabeledTables instance) {
    if (instance instanceof ImmutableLabeledTables) {
      return (ImmutableLabeledTables) instance;
    }
    return ImmutableLabeledTables.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableLabeledTables ImmutableLabeledTables}.
   * <pre>
   * ImmutableLabeledTables.builder()
   *    .putMap|putAllMap(String =&gt; io.deephaven.qst.table.TableSpec) // {@link LabeledTables#map() map} mappings
   *    .build();
   * </pre>
   * @return A new ImmutableLabeledTables builder
   */
  public static ImmutableLabeledTables.Builder builder() {
    return new ImmutableLabeledTables.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableLabeledTables ImmutableLabeledTables}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "LabeledTables", generator = "Immutables")
  public static final class Builder implements LabeledTables.Builder {
    private Map<String, TableSpec> map = new LinkedHashMap<String, TableSpec>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code LabeledTables} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(LabeledTables instance) {
      Objects.requireNonNull(instance, "instance");
      putAllMap(instance.map());
      return this;
    }

    /**
     * Put one entry to the {@link LabeledTables#map() map} map.
     * @param key The key in the map map
     * @param value The associated value in the map map
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder putMap(String key, TableSpec value) {
      this.map.put(
          Objects.requireNonNull(key, "map key"),
          value == null ? Objects.requireNonNull(value, "map value for key: " + key) : value);
      return this;
    }

    /**
     * Put one entry to the {@link LabeledTables#map() map} map. Nulls are not permitted
     * @param entry The key and value entry
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder putMap(Map.Entry<String, ? extends TableSpec> entry) {
      String k = entry.getKey();
      TableSpec v = entry.getValue();
      this.map.put(
          Objects.requireNonNull(k, "map key"),
          v == null ? Objects.requireNonNull(v, "map value for key: " + k) : v);
      return this;
    }

    /**
     * Sets or replaces all mappings from the specified map as entries for the {@link LabeledTables#map() map} map. Nulls are not permitted
     * @param entries The entries that will be added to the map map
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder map(Map<String, ? extends TableSpec> entries) {
      this.map.clear();
      return putAllMap(entries);
    }

    /**
     * Put all mappings from the specified map as entries to {@link LabeledTables#map() map} map. Nulls are not permitted
     * @param entries The entries that will be added to the map map
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder putAllMap(Map<String, ? extends TableSpec> entries) {
      for (Map.Entry<String, ? extends TableSpec> e : entries.entrySet()) {
        String k = e.getKey();
        TableSpec v = e.getValue();
        this.map.put(
            Objects.requireNonNull(k, "map key"),
            v == null ? Objects.requireNonNull(v, "map value for key: " + k) : v);
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableLabeledTables ImmutableLabeledTables}.
     * @return An immutable instance of LabeledTables
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableLabeledTables build() {
      return new ImmutableLabeledTables(createUnmodifiableMap(false, false, map));
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
