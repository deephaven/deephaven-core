package io.deephaven.process;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Represents the loggable environment variables as collected via {@link System#getenv()}.
 */
@Generated(from = "_EnvironmentVariables", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class EnvironmentVariables extends io.deephaven.process._EnvironmentVariables {
  private final Map<String, String> value;

  private EnvironmentVariables(Map<String, ? extends String> value) {
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
   * This instance is equal to all instances of {@code EnvironmentVariables} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof EnvironmentVariables
        && equalTo((EnvironmentVariables) another);
  }

  private boolean equalTo(EnvironmentVariables another) {
    return value.equals(another.value);
  }

  /**
   * Prints the immutable value {@code EnvironmentVariables} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "EnvironmentVariables{"
        + "value=" + value
        + "}";
  }

  /**
   * Construct a new immutable {@code EnvironmentVariables} instance.
   * @param value The value for the {@code value} attribute
   * @return An immutable EnvironmentVariables instance
   */
  public static EnvironmentVariables of(Map<String, ? extends String> value) {
    return new EnvironmentVariables(value);
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
