package io.deephaven.process;

import java.util.Objects;
import org.immutables.value.Generated;

/**
 * The globally unique ID for a process. This is <b>not</b> the same as a "pid" / process-id.
 */
@Generated(from = "_ProcessUniqueId", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ProcessUniqueId extends io.deephaven.process._ProcessUniqueId {
  private final String value;

  private ProcessUniqueId(String value) {
    this.value = Objects.requireNonNull(value, "value");
  }

  /**
   * @return The value of the {@code value} attribute
   */
  @Override
  public String value() {
    return value;
  }

  /**
   * This instance is equal to all instances of {@code ProcessUniqueId} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ProcessUniqueId
        && equalTo((ProcessUniqueId) another);
  }

  private boolean equalTo(ProcessUniqueId another) {
    return value.equals(another.value);
  }

  /**
   * Prints the immutable value {@code ProcessUniqueId} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ProcessUniqueId{"
        + "value=" + value
        + "}";
  }

  /**
   * Construct a new immutable {@code ProcessUniqueId} instance.
   * @param value The value for the {@code value} attribute
   * @return An immutable ProcessUniqueId instance
   */
  public static ProcessUniqueId of(String value) {
    return new ProcessUniqueId(value);
  }
}
