package io.deephaven.qst.table;

import io.deephaven.qst.array.Array;
import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link NewTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableNewTable.builder()}.
 */
@Generated(from = "NewTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableNewTable extends NewTable {
  private final int depth;
  private final Map<String, Array<?>> columns;
  private final int size;

  private ImmutableNewTable(ImmutableNewTable.Builder builder) {
    this.columns = createUnmodifiableMap(false, false, builder.columns);
    this.size = builder.size;
    this.depth = super.depth();
  }

  /**
   * @return The computed-at-construction value of the {@code depth} attribute
   */
  @Override
  public int depth() {
    return depth;
  }

  /**
   * @return The value of the {@code columns} attribute
   */
  @Override
  Map<String, Array<?>> columns() {
    return columns;
  }

  /**
   * @return The value of the {@code size} attribute
   */
  @Override
  public int size() {
    return size;
  }

  /**
   * This instance is equal to all instances of {@code ImmutableNewTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableNewTable
        && equalTo(0, (ImmutableNewTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableNewTable another) {
    return depth == another.depth
        && columns.equals(another.columns)
        && size == another.size;
  }

  /**
   * Computes a hash code from attributes: {@code depth}, {@code columns}, {@code size}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + getClass().hashCode();
    h += (h << 5) + depth;
    h += (h << 5) + columns.hashCode();
    h += (h << 5) + size;
    return h;
  }

  private static ImmutableNewTable validate(ImmutableNewTable instance) {
    instance.checkNames();
    instance.checkColumnsSizes();
    return instance;
  }

  private Object readResolve() throws ObjectStreamException {
    return validate(this);
  }

  /**
   * Creates a builder for {@link ImmutableNewTable ImmutableNewTable}.
   * <pre>
   * ImmutableNewTable.builder()
   *    .putColumns|putAllColumns(String =&gt; io.deephaven.qst.array.Array&amp;lt;?&amp;gt;) // {@link NewTable#columns() columns} mappings
   *    .size(int) // required {@link NewTable#size() size}
   *    .build();
   * </pre>
   * @return A new ImmutableNewTable builder
   */
  public static ImmutableNewTable.Builder builder() {
    return new ImmutableNewTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableNewTable ImmutableNewTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "NewTable", generator = "Immutables")
  public static final class Builder implements NewTable.Builder {
    private static final long INIT_BIT_SIZE = 0x1L;
    private long initBits = 0x1L;

    private final Map<String, Array<?>> columns = new LinkedHashMap<String, Array<?>>();
    private int size;

    private Builder() {
    }

    /**
     * Put one entry to the {@link NewTable#columns() columns} map.
     * @param key The key in the columns map
     * @param value The associated value in the columns map
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder putColumns(String key, Array<?> value) {
      this.columns.put(
          Objects.requireNonNull(key, "columns key"),
          value == null ? Objects.requireNonNull(value, "columns value for key: " + key) : value);
      return this;
    }

    /**
     * Put one entry to the {@link NewTable#columns() columns} map. Nulls are not permitted
     * @param entry The key and value entry
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder putColumns(Map.Entry<String, ? extends Array<?>> entry) {
      String k = entry.getKey();
      Array<?> v = entry.getValue();
      this.columns.put(
          Objects.requireNonNull(k, "columns key"),
          v == null ? Objects.requireNonNull(v, "columns value for key: " + k) : v);
      return this;
    }

    /**
     * Put all mappings from the specified map as entries to {@link NewTable#columns() columns} map. Nulls are not permitted
     * @param entries The entries that will be added to the columns map
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder putAllColumns(Map<String, ? extends Array<?>> entries) {
      for (Map.Entry<String, ? extends Array<?>> e : entries.entrySet()) {
        String k = e.getKey();
        Array<?> v = e.getValue();
        this.columns.put(
            Objects.requireNonNull(k, "columns key"),
            v == null ? Objects.requireNonNull(v, "columns value for key: " + k) : v);
      }
      return this;
    }

    /**
     * Initializes the value for the {@link NewTable#size() size} attribute.
     * @param size The value for size 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder size(int size) {
      checkNotIsSet(sizeIsSet(), "size");
      this.size = size;
      initBits &= ~INIT_BIT_SIZE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableNewTable ImmutableNewTable}.
     * @return An immutable instance of NewTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableNewTable build() {
      checkRequiredAttributes();
      return ImmutableNewTable.validate(new ImmutableNewTable(this));
    }

    private boolean sizeIsSet() {
      return (initBits & INIT_BIT_SIZE) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of NewTable is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!sizeIsSet()) attributes.add("size");
      return "Cannot build NewTable, some of required attributes are not set " + attributes;
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
