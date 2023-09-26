package io.deephaven.qst.table;

import io.deephaven.qst.type.Type;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link TableHeader}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableTableHeader.builder()}.
 */
@Generated(from = "TableHeader", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableTableHeader extends TableHeader {
  private final Map<String, Type<?>> headers;

  private ImmutableTableHeader(Map<String, Type<?>> headers) {
    this.headers = headers;
  }

  /**
   * @return The value of the {@code headers} attribute
   */
  @Override
  Map<String, Type<?>> headers() {
    return headers;
  }

  /**
   * Copy the current immutable object by replacing the {@link TableHeader#headers() headers} map with the specified map.
   * Nulls are not permitted as keys or values.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param entries The entries to be added to the headers map
   * @return A modified copy of {@code this} object
   */
  public final ImmutableTableHeader withHeaders(Map<String, ? extends Type<?>> entries) {
    if (this.headers == entries) return this;
    Map<String, Type<?>> newValue = createUnmodifiableMap(true, false, entries);
    return validate(new ImmutableTableHeader(newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableTableHeader} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableTableHeader
        && equalTo(0, (ImmutableTableHeader) another);
  }

  private boolean equalTo(int synthetic, ImmutableTableHeader another) {
    return headers.equals(another.headers);
  }

  /**
   * Computes a hash code from attributes: {@code headers}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + headers.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code TableHeader} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "TableHeader{"
        + "headers=" + headers
        + "}";
  }

  private static ImmutableTableHeader validate(ImmutableTableHeader instance) {
    instance.checkNames();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link TableHeader} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable TableHeader instance
   */
  public static ImmutableTableHeader copyOf(TableHeader instance) {
    if (instance instanceof ImmutableTableHeader) {
      return (ImmutableTableHeader) instance;
    }
    return ImmutableTableHeader.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableTableHeader ImmutableTableHeader}.
   * <pre>
   * ImmutableTableHeader.builder()
   *    .putHeaders|putAllHeaders(String =&gt; io.deephaven.qst.type.Type&amp;lt;?&amp;gt;) // {@link TableHeader#headers() headers} mappings
   *    .build();
   * </pre>
   * @return A new ImmutableTableHeader builder
   */
  public static ImmutableTableHeader.Builder builder() {
    return new ImmutableTableHeader.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableTableHeader ImmutableTableHeader}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "TableHeader", generator = "Immutables")
  public static final class Builder implements TableHeader.Builder {
    private Map<String, Type<?>> headers = new LinkedHashMap<String, Type<?>>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code TableHeader} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(TableHeader instance) {
      Objects.requireNonNull(instance, "instance");
      putAllHeaders(instance.headers());
      return this;
    }

    /**
     * Put one entry to the {@link TableHeader#headers() headers} map.
     * @param key The key in the headers map
     * @param value The associated value in the headers map
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder putHeaders(String key, Type<?> value) {
      this.headers.put(
          Objects.requireNonNull(key, "headers key"),
          value == null ? Objects.requireNonNull(value, "headers value for key: " + key) : value);
      return this;
    }

    /**
     * Put one entry to the {@link TableHeader#headers() headers} map. Nulls are not permitted
     * @param entry The key and value entry
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder putHeaders(Map.Entry<String, ? extends Type<?>> entry) {
      String k = entry.getKey();
      Type<?> v = entry.getValue();
      this.headers.put(
          Objects.requireNonNull(k, "headers key"),
          v == null ? Objects.requireNonNull(v, "headers value for key: " + k) : v);
      return this;
    }

    /**
     * Sets or replaces all mappings from the specified map as entries for the {@link TableHeader#headers() headers} map. Nulls are not permitted
     * @param entries The entries that will be added to the headers map
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder headers(Map<String, ? extends Type<?>> entries) {
      this.headers.clear();
      return putAllHeaders(entries);
    }

    /**
     * Put all mappings from the specified map as entries to {@link TableHeader#headers() headers} map. Nulls are not permitted
     * @param entries The entries that will be added to the headers map
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder putAllHeaders(Map<String, ? extends Type<?>> entries) {
      for (Map.Entry<String, ? extends Type<?>> e : entries.entrySet()) {
        String k = e.getKey();
        Type<?> v = e.getValue();
        this.headers.put(
            Objects.requireNonNull(k, "headers key"),
            v == null ? Objects.requireNonNull(v, "headers value for key: " + k) : v);
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableTableHeader ImmutableTableHeader}.
     * @return An immutable instance of TableHeader
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableTableHeader build() {
      return ImmutableTableHeader.validate(new ImmutableTableHeader(createUnmodifiableMap(false, false, headers)));
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
