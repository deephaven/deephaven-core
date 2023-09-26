package io.deephaven.process;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link _ApplicationArguments}.
 * <p>
 * Use the static factory method to create immutable instances:
 * {@code ApplicationArguments.of()}.
 */
@Generated(from = "_ApplicationArguments", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ApplicationArguments extends io.deephaven.process._ApplicationArguments {
  private final List<String> value;

  private ApplicationArguments(Iterable<String> value) {
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
   * This instance is equal to all instances of {@code ApplicationArguments} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ApplicationArguments
        && equalTo((ApplicationArguments) another);
  }

  private boolean equalTo(ApplicationArguments another) {
    return value.equals(another.value);
  }

  /**
   * Prints the immutable value {@code ApplicationArguments} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ApplicationArguments{"
        + "value=" + value
        + "}";
  }

  /**
   * Construct a new immutable {@code ApplicationArguments} instance.
   * @param value The value for the {@code value} attribute
   * @return An immutable ApplicationArguments instance
   */
  public static ApplicationArguments of(List<String> value) {
    return of((Iterable<String>) value);
  }

  /**
   * Construct a new immutable {@code ApplicationArguments} instance.
   * @param value The value for the {@code value} attribute
   * @return An immutable ApplicationArguments instance
   */
  public static ApplicationArguments of(Iterable<String> value) {
    return new ApplicationArguments(value);
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
