package io.deephaven.process;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Represents the system properties as collected via {@link RuntimeMXBean#getSystemProperties()}.
 */
@Generated(from = "_SystemProperties", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class SystemProperties extends io.deephaven.process._SystemProperties {
  private final Map<String, String> value;

  private SystemProperties(Map<String, ? extends String> value) {
    this.value = createUnmodifiableMap(true, false, value);
  }

  /**
   * @return The value of the {@code value} attribute
   */
  @Override
  public Map<String, String> value() {
    return value;
  }

  /**
   * This instance is equal to all instances of {@code SystemProperties} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof SystemProperties
        && equalTo(0, (SystemProperties) another);
  }

  private boolean equalTo(int synthetic, SystemProperties another) {
    return value.equals(another.value);
  }

  /**
   * Prints the immutable value {@code SystemProperties} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "SystemProperties{"
        + "value=" + value
        + "}";
  }

  /**
   * Construct a new immutable {@code SystemProperties} instance.
   * @param value The value for the {@code value} attribute
   * @return An immutable SystemProperties instance
   */
  public static SystemProperties of(Map<String, ? extends String> value) {
    return new SystemProperties(value);
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
