package io.deephaven.process;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link MemoryPoolsMxBeanInfo}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableMemoryPoolsMxBeanInfo.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableMemoryPoolsMxBeanInfo.of()}.
 */
@Generated(from = "MemoryPoolsMxBeanInfo", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableMemoryPoolsMxBeanInfo extends MemoryPoolsMxBeanInfo {
  private final Map<String, MemoryUsageInfo> usages;

  private ImmutableMemoryPoolsMxBeanInfo(Map<String, ? extends MemoryUsageInfo> usages) {
    this.usages = createUnmodifiableMap(true, false, usages);
  }

  private ImmutableMemoryPoolsMxBeanInfo(ImmutableMemoryPoolsMxBeanInfo.Builder builder) {
    this.usages = createUnmodifiableMap(false, false, builder.usages);
  }

  /**
   * @return The value of the {@code usages} attribute
   */
  @Override
  public Map<String, MemoryUsageInfo> usages() {
    return usages;
  }

  /**
   * This instance is equal to all instances of {@code ImmutableMemoryPoolsMxBeanInfo} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableMemoryPoolsMxBeanInfo
        && equalTo((ImmutableMemoryPoolsMxBeanInfo) another);
  }

  private boolean equalTo(ImmutableMemoryPoolsMxBeanInfo another) {
    return usages.equals(another.usages);
  }

  /**
   * Computes a hash code from attributes: {@code usages}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + usages.hashCode();
    return h;
  }


  /**
   * Prints the immutable value {@code MemoryPoolsMxBeanInfo} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "MemoryPoolsMxBeanInfo{"
        + "usages=" + usages
        + "}";
  }

  /**
   * Construct a new immutable {@code MemoryPoolsMxBeanInfo} instance.
   * @param usages The value for the {@code usages} attribute
   * @return An immutable MemoryPoolsMxBeanInfo instance
   */
  public static ImmutableMemoryPoolsMxBeanInfo of(Map<String, ? extends MemoryUsageInfo> usages) {
    return new ImmutableMemoryPoolsMxBeanInfo(usages);
  }

  /**
   * Creates a builder for {@link ImmutableMemoryPoolsMxBeanInfo ImmutableMemoryPoolsMxBeanInfo}.
   * <pre>
   * ImmutableMemoryPoolsMxBeanInfo.builder()
   *    .putUsages|putAllUsages(String =&gt; io.deephaven.process.MemoryUsageInfo) // {@link MemoryPoolsMxBeanInfo#usages() usages} mappings
   *    .build();
   * </pre>
   * @return A new ImmutableMemoryPoolsMxBeanInfo builder
   */
  public static ImmutableMemoryPoolsMxBeanInfo.Builder builder() {
    return new ImmutableMemoryPoolsMxBeanInfo.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableMemoryPoolsMxBeanInfo ImmutableMemoryPoolsMxBeanInfo}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "MemoryPoolsMxBeanInfo", generator = "Immutables")
  public static final class Builder {
    private final Map<String, MemoryUsageInfo> usages = new LinkedHashMap<String, MemoryUsageInfo>();

    private Builder() {
    }

    /**
     * Put one entry to the {@link MemoryPoolsMxBeanInfo#usages() usages} map.
     * @param key The key in the usages map
     * @param value The associated value in the usages map
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder putUsages(String key, MemoryUsageInfo value) {
      this.usages.put(
          Objects.requireNonNull(key, "usages key"),
          Objects.requireNonNull(value, "usages value"));
      return this;
    }

    /**
     * Put one entry to the {@link MemoryPoolsMxBeanInfo#usages() usages} map. Nulls are not permitted
     * @param entry The key and value entry
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder putUsages(Map.Entry<String, ? extends MemoryUsageInfo> entry) {
      String k = entry.getKey();
      MemoryUsageInfo v = entry.getValue();
      this.usages.put(
          Objects.requireNonNull(k, "usages key"),
          Objects.requireNonNull(v, "usages value"));
      return this;
    }

    /**
     * Put all mappings from the specified map as entries to {@link MemoryPoolsMxBeanInfo#usages() usages} map. Nulls are not permitted
     * @param entries The entries that will be added to the usages map
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder putAllUsages(Map<String, ? extends MemoryUsageInfo> entries) {
      for (Map.Entry<String, ? extends MemoryUsageInfo> e : entries.entrySet()) {
        String k = e.getKey();
        MemoryUsageInfo v = e.getValue();
        this.usages.put(
            Objects.requireNonNull(k, "usages key"),
            Objects.requireNonNull(v, "usages value"));
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableMemoryPoolsMxBeanInfo ImmutableMemoryPoolsMxBeanInfo}.
     * @return An immutable instance of MemoryPoolsMxBeanInfo
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableMemoryPoolsMxBeanInfo build() {
      return new ImmutableMemoryPoolsMxBeanInfo(this);
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
