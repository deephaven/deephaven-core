package io.deephaven.qst.table;

import io.deephaven.qst.array.Array;
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
<<<<<<< HEAD
final class ImmutableNewTable extends NewTable {
=======
public final class ImmutableNewTable extends NewTable {
>>>>>>> main
  private transient final int depth;
  private final Map<String, Array<?>> columns;
  private final int size;

<<<<<<< HEAD
  private ImmutableNewTable(ImmutableNewTable.Builder builder) {
    this.columns = createUnmodifiableMap(false, false, builder.columns);
    this.size = builder.size;
=======
  private ImmutableNewTable(Map<String, Array<?>> columns, int size) {
    this.columns = columns;
    this.size = size;
>>>>>>> main
    this.depth = super.depth();
  }

  /**
<<<<<<< HEAD
   * @return The computed-at-construction value of the {@code depth} attribute
=======
   * The depth of the table is the maximum depth of its dependencies plus one. A table with no dependencies has a
   * depth of zero.
   * @return the depth
>>>>>>> main
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
<<<<<<< HEAD
=======
   * Copy the current immutable object by replacing the {@link NewTable#columns() columns} map with the specified map.
   * Nulls are not permitted as keys or values.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param entries The entries to be added to the columns map
   * @return A modified copy of {@code this} object
   */
  public final ImmutableNewTable withColumns(Map<String, ? extends Array<?>> entries) {
    if (this.columns == entries) return this;
    Map<String, Array<?>> newValue = createUnmodifiableMap(true, false, entries);
    return validate(new ImmutableNewTable(newValue, this.size));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link NewTable#size() size} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for size
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableNewTable withSize(int value) {
    if (this.size == value) return this;
    return validate(new ImmutableNewTable(this.columns, value));
  }

  /**
>>>>>>> main
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
<<<<<<< HEAD
    h += (h << 5) + getClass().hashCode();
=======
>>>>>>> main
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

  /**
<<<<<<< HEAD
=======
   * Creates an immutable copy of a {@link NewTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable NewTable instance
   */
  public static ImmutableNewTable copyOf(NewTable instance) {
    if (instance instanceof ImmutableNewTable) {
      return (ImmutableNewTable) instance;
    }
    return ImmutableNewTable.builder()
        .from(instance)
        .build();
  }

  /**
>>>>>>> main
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

<<<<<<< HEAD
    private final Map<String, Array<?>> columns = new LinkedHashMap<String, Array<?>>();
=======
    private Map<String, Array<?>> columns = new LinkedHashMap<String, Array<?>>();
>>>>>>> main
    private int size;

    private Builder() {
    }

    /**
<<<<<<< HEAD
=======
     * Fill a builder with attribute values from the provided {@code NewTable} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(NewTable instance) {
      Objects.requireNonNull(instance, "instance");
      putAllColumns(instance.columns());
      size(instance.size());
      return this;
    }

    /**
>>>>>>> main
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
<<<<<<< HEAD
=======
     * Sets or replaces all mappings from the specified map as entries for the {@link NewTable#columns() columns} map. Nulls are not permitted
     * @param entries The entries that will be added to the columns map
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder columns(Map<String, ? extends Array<?>> entries) {
      this.columns.clear();
      return putAllColumns(entries);
    }

    /**
>>>>>>> main
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
<<<<<<< HEAD
      checkNotIsSet(sizeIsSet(), "size");
=======
>>>>>>> main
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
<<<<<<< HEAD
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
=======
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableNewTable.validate(new ImmutableNewTable(createUnmodifiableMap(false, false, columns), size));
>>>>>>> main
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
<<<<<<< HEAD
      if (!sizeIsSet()) attributes.add("size");
=======
      if ((initBits & INIT_BIT_SIZE) != 0) attributes.add("size");
>>>>>>> main
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
