package io.deephaven.qst;

import io.deephaven.api.TableOperations;
import io.deephaven.qst.table.TableSpec;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link TableAdapterResults}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableTableAdapterResults.builder()}.
 */
@Generated(from = "TableAdapterResults", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableTableAdapterResults<TOPS extends TableOperations<TOPS, TABLE>, TABLE>
    extends TableAdapterResults<TOPS, TABLE> {
  private final Map<TableSpec, TableAdapterResults.Output<TOPS, TABLE>> map;

  private ImmutableTableAdapterResults(
      Map<TableSpec, TableAdapterResults.Output<TOPS, TABLE>> map) {
    this.map = map;
  }

  /**
   * @return The value of the {@code map} attribute
   */
  @Override
  public Map<TableSpec, TableAdapterResults.Output<TOPS, TABLE>> map() {
    return map;
  }

  /**
   * Copy the current immutable object by replacing the {@link TableAdapterResults#map() map} map with the specified map.
   * Nulls are not permitted as keys or values.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param entries The entries to be added to the map map
   * @return A modified copy of {@code this} object
   */
  public final ImmutableTableAdapterResults<TOPS, TABLE> withMap(Map<? extends TableSpec, ? extends TableAdapterResults.Output<TOPS, TABLE>> entries) {
    if (this.map == entries) return this;
    Map<TableSpec, TableAdapterResults.Output<TOPS, TABLE>> newValue = createUnmodifiableMap(true, false, entries);
    return new ImmutableTableAdapterResults<>(newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableTableAdapterResults} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableTableAdapterResults<?, ?>
        && equalTo(0, (ImmutableTableAdapterResults<?, ?>) another);
  }

  private boolean equalTo(int synthetic, ImmutableTableAdapterResults<?, ?> another) {
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
   * Prints the immutable value {@code TableAdapterResults} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "TableAdapterResults{"
        + "map=" + map
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link TableAdapterResults} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param <TOPS> generic parameter TOPS
   * @param <TABLE> generic parameter TABLE
   * @param instance The instance to copy
   * @return A copied immutable TableAdapterResults instance
   */
  public static <TOPS extends TableOperations<TOPS, TABLE>, TABLE> ImmutableTableAdapterResults<TOPS, TABLE> copyOf(TableAdapterResults<TOPS, TABLE> instance) {
    if (instance instanceof ImmutableTableAdapterResults<?, ?>) {
      return (ImmutableTableAdapterResults<TOPS, TABLE>) instance;
    }
    return ImmutableTableAdapterResults.<TOPS, TABLE>builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableTableAdapterResults ImmutableTableAdapterResults}.
   * <pre>
   * ImmutableTableAdapterResults.&amp;lt;TOPS, TABLE&amp;gt;builder()
   *    .putMap|putAllMap(io.deephaven.qst.table.TableSpec =&gt; io.deephaven.qst.TableAdapterResults.Output&amp;lt;TOPS, TABLE&amp;gt;) // {@link TableAdapterResults#map() map} mappings
   *    .build();
   * </pre>
   * @param <TOPS> generic parameter TOPS
   * @param <TABLE> generic parameter TABLE
   * @return A new ImmutableTableAdapterResults builder
   */
  public static <TOPS extends TableOperations<TOPS, TABLE>, TABLE> ImmutableTableAdapterResults.Builder<TOPS, TABLE> builder() {
    return new ImmutableTableAdapterResults.Builder<>();
  }

  /**
   * Builds instances of type {@link ImmutableTableAdapterResults ImmutableTableAdapterResults}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "TableAdapterResults", generator = "Immutables")
  public static final class Builder<TOPS extends TableOperations<TOPS, TABLE>, TABLE> {
    private Map<TableSpec, TableAdapterResults.Output<TOPS, TABLE>> map = new LinkedHashMap<TableSpec, TableAdapterResults.Output<TOPS, TABLE>>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code TableAdapterResults} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<TOPS, TABLE> from(TableAdapterResults<TOPS, TABLE> instance) {
      Objects.requireNonNull(instance, "instance");
      putAllMap(instance.map());
      return this;
    }

    /**
     * Put one entry to the {@link TableAdapterResults#map() map} map.
     * @param key The key in the map map
     * @param value The associated value in the map map
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<TOPS, TABLE> putMap(TableSpec key, TableAdapterResults.Output<TOPS, TABLE> value) {
      this.map.put(
          Objects.requireNonNull(key, "map key"),
          value == null ? Objects.requireNonNull(value, "map value for key: " + key) : value);
      return this;
    }

    /**
     * Put one entry to the {@link TableAdapterResults#map() map} map. Nulls are not permitted
     * @param entry The key and value entry
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<TOPS, TABLE> putMap(Map.Entry<? extends TableSpec, ? extends TableAdapterResults.Output<TOPS, TABLE>> entry) {
      TableSpec k = entry.getKey();
      TableAdapterResults.Output<TOPS, TABLE> v = entry.getValue();
      this.map.put(
          Objects.requireNonNull(k, "map key"),
          v == null ? Objects.requireNonNull(v, "map value for key: " + k) : v);
      return this;
    }

    /**
     * Sets or replaces all mappings from the specified map as entries for the {@link TableAdapterResults#map() map} map. Nulls are not permitted
     * @param entries The entries that will be added to the map map
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<TOPS, TABLE> map(Map<? extends TableSpec, ? extends TableAdapterResults.Output<TOPS, TABLE>> entries) {
      this.map.clear();
      return putAllMap(entries);
    }

    /**
     * Put all mappings from the specified map as entries to {@link TableAdapterResults#map() map} map. Nulls are not permitted
     * @param entries The entries that will be added to the map map
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<TOPS, TABLE> putAllMap(Map<? extends TableSpec, ? extends TableAdapterResults.Output<TOPS, TABLE>> entries) {
      for (Map.Entry<? extends TableSpec, ? extends TableAdapterResults.Output<TOPS, TABLE>> e : entries.entrySet()) {
        TableSpec k = e.getKey();
        TableAdapterResults.Output<TOPS, TABLE> v = e.getValue();
        this.map.put(
            Objects.requireNonNull(k, "map key"),
            v == null ? Objects.requireNonNull(v, "map value for key: " + k) : v);
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableTableAdapterResults ImmutableTableAdapterResults}.
     * @return An immutable instance of TableAdapterResults
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableTableAdapterResults<TOPS, TABLE> build() {
      return new ImmutableTableAdapterResults<>(createUnmodifiableMap(false, false, map));
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
