package io.deephaven.process;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link MemoryMxBeanInfo}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableMemoryMxBeanInfo.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableMemoryMxBeanInfo.of()}.
 */
@Generated(from = "MemoryMxBeanInfo", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableMemoryMxBeanInfo extends MemoryMxBeanInfo {
  private final MemoryUsageInfo heap;
  private final MemoryUsageInfo nonHeap;

  private ImmutableMemoryMxBeanInfo(MemoryUsageInfo heap, MemoryUsageInfo nonHeap) {
    this.heap = Objects.requireNonNull(heap, "heap");
    this.nonHeap = Objects.requireNonNull(nonHeap, "nonHeap");
  }

  private ImmutableMemoryMxBeanInfo(ImmutableMemoryMxBeanInfo.Builder builder) {
    this.heap = builder.heap;
    this.nonHeap = builder.nonHeap;
  }

  /**
   * @return The value of the {@code heap} attribute
   */
  @Override
  public MemoryUsageInfo heap() {
    return heap;
  }

  /**
   * @return The value of the {@code nonHeap} attribute
   */
  @Override
  public MemoryUsageInfo nonHeap() {
    return nonHeap;
  }

  /**
   * This instance is equal to all instances of {@code ImmutableMemoryMxBeanInfo} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableMemoryMxBeanInfo
        && equalTo((ImmutableMemoryMxBeanInfo) another);
  }

  private boolean equalTo(ImmutableMemoryMxBeanInfo another) {
    return heap.equals(another.heap)
        && nonHeap.equals(another.nonHeap);
  }

  /**
   * Computes a hash code from attributes: {@code heap}, {@code nonHeap}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + heap.hashCode();
    h += (h << 5) + nonHeap.hashCode();
    return h;
  }


  /**
   * Prints the immutable value {@code MemoryMxBeanInfo} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "MemoryMxBeanInfo{"
        + "heap=" + heap
        + ", nonHeap=" + nonHeap
        + "}";
  }

  /**
   * Construct a new immutable {@code MemoryMxBeanInfo} instance.
   * @param heap The value for the {@code heap} attribute
   * @param nonHeap The value for the {@code nonHeap} attribute
   * @return An immutable MemoryMxBeanInfo instance
   */
  public static ImmutableMemoryMxBeanInfo of(MemoryUsageInfo heap, MemoryUsageInfo nonHeap) {
    return new ImmutableMemoryMxBeanInfo(heap, nonHeap);
  }

  /**
   * Creates a builder for {@link ImmutableMemoryMxBeanInfo ImmutableMemoryMxBeanInfo}.
   * <pre>
   * ImmutableMemoryMxBeanInfo.builder()
   *    .heap(io.deephaven.process.MemoryUsageInfo) // required {@link MemoryMxBeanInfo#heap() heap}
   *    .nonHeap(io.deephaven.process.MemoryUsageInfo) // required {@link MemoryMxBeanInfo#nonHeap() nonHeap}
   *    .build();
   * </pre>
   * @return A new ImmutableMemoryMxBeanInfo builder
   */
  public static ImmutableMemoryMxBeanInfo.Builder builder() {
    return new ImmutableMemoryMxBeanInfo.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableMemoryMxBeanInfo ImmutableMemoryMxBeanInfo}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "MemoryMxBeanInfo", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_HEAP = 0x1L;
    private static final long INIT_BIT_NON_HEAP = 0x2L;
    private long initBits = 0x3L;

    private MemoryUsageInfo heap;
    private MemoryUsageInfo nonHeap;

    private Builder() {
    }

    /**
     * Initializes the value for the {@link MemoryMxBeanInfo#heap() heap} attribute.
     * @param heap The value for heap 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder heap(MemoryUsageInfo heap) {
      checkNotIsSet(heapIsSet(), "heap");
      this.heap = Objects.requireNonNull(heap, "heap");
      initBits &= ~INIT_BIT_HEAP;
      return this;
    }

    /**
     * Initializes the value for the {@link MemoryMxBeanInfo#nonHeap() nonHeap} attribute.
     * @param nonHeap The value for nonHeap 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder nonHeap(MemoryUsageInfo nonHeap) {
      checkNotIsSet(nonHeapIsSet(), "nonHeap");
      this.nonHeap = Objects.requireNonNull(nonHeap, "nonHeap");
      initBits &= ~INIT_BIT_NON_HEAP;
      return this;
    }

    /**
     * Builds a new {@link ImmutableMemoryMxBeanInfo ImmutableMemoryMxBeanInfo}.
     * @return An immutable instance of MemoryMxBeanInfo
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableMemoryMxBeanInfo build() {
      checkRequiredAttributes();
      return new ImmutableMemoryMxBeanInfo(this);
    }

    private boolean heapIsSet() {
      return (initBits & INIT_BIT_HEAP) == 0;
    }

    private boolean nonHeapIsSet() {
      return (initBits & INIT_BIT_NON_HEAP) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of MemoryMxBeanInfo is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!heapIsSet()) attributes.add("heap");
      if (!nonHeapIsSet()) attributes.add("nonHeap");
      return "Cannot build MemoryMxBeanInfo, some of required attributes are not set " + attributes;
    }
  }
}
