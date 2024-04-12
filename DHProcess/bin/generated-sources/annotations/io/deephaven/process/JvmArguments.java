package io.deephaven.process;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Represents the JVM input arguments as collected via {@link RuntimeMXBean#getInputArguments()}.
 */
@Generated(from = "_JvmArguments", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class JvmArguments extends io.deephaven.process._JvmArguments {
  private final List<String> value;

  private JvmArguments(Iterable<String> value) {
    this.value = createUnmodifiableList(false, createSafeList(value, true, false));
  }

  /**
   * @return The value of the {@code value} attribute
   */
  @Override
  public List<String> value() {
    return value;
  }

  /**
   * This instance is equal to all instances of {@code JvmArguments} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof JvmArguments
        && equalTo((JvmArguments) another);
  }

  private boolean equalTo(JvmArguments another) {
    return value.equals(another.value);
  }

  /**
   * Prints the immutable value {@code JvmArguments} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "JvmArguments{"
        + "value=" + value
        + "}";
  }

  /**
   * Construct a new immutable {@code JvmArguments} instance.
   * @param value The value for the {@code value} attribute
   * @return An immutable JvmArguments instance
   */
  public static JvmArguments of(List<String> value) {
    return of((Iterable<String>) value);
  }

  /**
   * Construct a new immutable {@code JvmArguments} instance.
   * @param value The value for the {@code value} attribute
   * @return An immutable JvmArguments instance
   */
  public static JvmArguments of(Iterable<String> value) {
    return new JvmArguments(value);
  }

  private static <T> List<T> createSafeList(Iterable<? extends T> iterable, boolean checkNulls, boolean skipNulls) {
    ArrayList<T> list;
    if (iterable instanceof Collection<?>) {
      int size = ((Collection<?>) iterable).size();
      if (size == 0) return Collections.emptyList();
      list = new ArrayList<>();
    } else {
      list = new ArrayList<>();
    }
    for (T element : iterable) {
      if (skipNulls && element == null) continue;
      if (checkNulls) Objects.requireNonNull(element, "element");
      list.add(element);
    }
    return list;
  }

  private static <T> List<T> createUnmodifiableList(boolean clone, List<T> list) {
    switch(list.size()) {
    case 0: return Collections.emptyList();
    case 1: return Collections.singletonList(list.get(0));
    default:
      if (clone) {
        return Collections.unmodifiableList(new ArrayList<>(list));
      } else {
        if (list instanceof ArrayList<?>) {
          ((ArrayList<?>) list).trimToSize();
        }
        return Collections.unmodifiableList(list);
      }
    }
  }
}
