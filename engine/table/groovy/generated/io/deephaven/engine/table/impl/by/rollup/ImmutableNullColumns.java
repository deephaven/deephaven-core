package io.deephaven.engine.table.impl.by.rollup;

import java.io.ObjectStreamException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link NullColumns}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableNullColumns.builder()}.
 */
@Generated(from = "NullColumns", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
final class ImmutableNullColumns extends NullColumns {
  private final Map<String, Class<?>> resultColumns;

  private ImmutableNullColumns(ImmutableNullColumns.Builder builder) {
    this.resultColumns = createUnmodifiableMap(false, false, builder.resultColumns);
  }

  /**
   * @return The value of the {@code resultColumns} attribute
   */
  @Override
  public Map<String, Class<?>> resultColumns() {
    return resultColumns;
  }

  /**
   * This instance is equal to all instances of {@code ImmutableNullColumns} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableNullColumns
        && equalTo((ImmutableNullColumns) another);
  }

  private boolean equalTo(ImmutableNullColumns another) {
    return resultColumns.equals(another.resultColumns);
  }

  /**
   * Computes a hash code from attributes: {@code resultColumns}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + resultColumns.hashCode();
    return h;
  }


  /**
   * Prints the immutable value {@code NullColumns} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "NullColumns{"
        + "resultColumns=" + resultColumns
        + "}";
  }

  private static ImmutableNullColumns validate(ImmutableNullColumns instance) {
    instance.checkNonEmpty();
    return instance;
  }

  private Object readResolve() throws ObjectStreamException {
    return validate(this);
  }

  /**
   * Creates a builder for {@link ImmutableNullColumns ImmutableNullColumns}.
   * <pre>
   * ImmutableNullColumns.builder()
   *    .putResultColumns|putAllResultColumns(String =&gt; Class&amp;lt;?&amp;gt;) // {@link NullColumns#resultColumns() resultColumns} mappings
   *    .build();
   * </pre>
   * @return A new ImmutableNullColumns builder
   */
  public static ImmutableNullColumns.Builder builder() {
    return new ImmutableNullColumns.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableNullColumns ImmutableNullColumns}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "NullColumns", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements NullColumns.Builder {
    private final Map<String, Class<?>> resultColumns = new LinkedHashMap<String, Class<?>>();

    private Builder() {
    }

    /**
     * Put one entry to the {@link NullColumns#resultColumns() resultColumns} map.
     * @param key The key in the resultColumns map
     * @param value The associated value in the resultColumns map
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder putResultColumns(String key, Class<?> value) {
      this.resultColumns.put(
          Objects.requireNonNull(key, "resultColumns key"),
          Objects.requireNonNull(value, "resultColumns value"));
      return this;
    }

    /**
     * Put one entry to the {@link NullColumns#resultColumns() resultColumns} map. Nulls are not permitted
     * @param entry The key and value entry
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder putResultColumns(Map.Entry<String, ? extends Class<?>> entry) {
      String k = entry.getKey();
      Class<?> v = entry.getValue();
      this.resultColumns.put(
          Objects.requireNonNull(k, "resultColumns key"),
          Objects.requireNonNull(v, "resultColumns value"));
      return this;
    }

    /**
     * Put all mappings from the specified map as entries to {@link NullColumns#resultColumns() resultColumns} map. Nulls are not permitted
     * @param entries The entries that will be added to the resultColumns map
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder putAllResultColumns(Map<String, ? extends Class<?>> entries) {
      for (Map.Entry<String, ? extends Class<?>> e : entries.entrySet()) {
        String k = e.getKey();
        Class<?> v = e.getValue();
        this.resultColumns.put(
            Objects.requireNonNull(k, "resultColumns key"),
            Objects.requireNonNull(v, "resultColumns value"));
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableNullColumns ImmutableNullColumns}.
     * @return An immutable instance of NullColumns
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableNullColumns build() {
      return ImmutableNullColumns.validate(new ImmutableNullColumns(this));
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
        Objects.requireNonNull(v, "value");
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
            Objects.requireNonNull(v, "value");
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
