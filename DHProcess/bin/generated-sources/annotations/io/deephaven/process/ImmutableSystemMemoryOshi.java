package io.deephaven.process;

import java.util.ArrayList;
import java.util.List;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link SystemMemoryOshi}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableSystemMemoryOshi.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableSystemMemoryOshi.of()}.
 */
@Generated(from = "SystemMemoryOshi", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableSystemMemoryOshi extends SystemMemoryOshi {
  private final long physicalTotal;
  private final long swapTotal;
  private final long pageSize;

  private ImmutableSystemMemoryOshi(long physicalTotal, long swapTotal, long pageSize) {
    this.physicalTotal = physicalTotal;
    this.swapTotal = swapTotal;
    this.pageSize = pageSize;
  }

  private ImmutableSystemMemoryOshi(ImmutableSystemMemoryOshi.Builder builder) {
    this.physicalTotal = builder.physicalTotal;
    this.swapTotal = builder.swapTotal;
    this.pageSize = builder.pageSize;
  }

  /**
   * @return The value of the {@code physicalTotal} attribute
   */
  @Override
  public long getPhysicalTotal() {
    return physicalTotal;
  }

  /**
   * @return The value of the {@code swapTotal} attribute
   */
  @Override
  public long getSwapTotal() {
    return swapTotal;
  }

  /**
   * @return The value of the {@code pageSize} attribute
   */
  @Override
  public long getPageSize() {
    return pageSize;
  }

  /**
   * This instance is equal to all instances of {@code ImmutableSystemMemoryOshi} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableSystemMemoryOshi
        && equalTo((ImmutableSystemMemoryOshi) another);
  }

  private boolean equalTo(ImmutableSystemMemoryOshi another) {
    return physicalTotal == another.physicalTotal
        && swapTotal == another.swapTotal
        && pageSize == another.pageSize;
  }

  /**
   * Computes a hash code from attributes: {@code physicalTotal}, {@code swapTotal}, {@code pageSize}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Long.hashCode(physicalTotal);
    h += (h << 5) + Long.hashCode(swapTotal);
    h += (h << 5) + Long.hashCode(pageSize);
    return h;
  }


  /**
   * Prints the immutable value {@code SystemMemoryOshi} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "SystemMemoryOshi{"
        + "physicalTotal=" + physicalTotal
        + ", swapTotal=" + swapTotal
        + ", pageSize=" + pageSize
        + "}";
  }

  /**
   * Construct a new immutable {@code SystemMemoryOshi} instance.
   * @param physicalTotal The value for the {@code physicalTotal} attribute
   * @param swapTotal The value for the {@code swapTotal} attribute
   * @param pageSize The value for the {@code pageSize} attribute
   * @return An immutable SystemMemoryOshi instance
   */
  public static ImmutableSystemMemoryOshi of(long physicalTotal, long swapTotal, long pageSize) {
    return new ImmutableSystemMemoryOshi(physicalTotal, swapTotal, pageSize);
  }

  /**
   * Creates a builder for {@link ImmutableSystemMemoryOshi ImmutableSystemMemoryOshi}.
   * <pre>
   * ImmutableSystemMemoryOshi.builder()
   *    .physicalTotal(long) // required {@link SystemMemoryOshi#getPhysicalTotal() physicalTotal}
   *    .swapTotal(long) // required {@link SystemMemoryOshi#getSwapTotal() swapTotal}
   *    .pageSize(long) // required {@link SystemMemoryOshi#getPageSize() pageSize}
   *    .build();
   * </pre>
   * @return A new ImmutableSystemMemoryOshi builder
   */
  public static ImmutableSystemMemoryOshi.Builder builder() {
    return new ImmutableSystemMemoryOshi.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableSystemMemoryOshi ImmutableSystemMemoryOshi}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "SystemMemoryOshi", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_PHYSICAL_TOTAL = 0x1L;
    private static final long INIT_BIT_SWAP_TOTAL = 0x2L;
    private static final long INIT_BIT_PAGE_SIZE = 0x4L;
    private long initBits = 0x7L;

    private long physicalTotal;
    private long swapTotal;
    private long pageSize;

    private Builder() {
    }

    /**
     * Initializes the value for the {@link SystemMemoryOshi#getPhysicalTotal() physicalTotal} attribute.
     * @param physicalTotal The value for physicalTotal 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder physicalTotal(long physicalTotal) {
      checkNotIsSet(physicalTotalIsSet(), "physicalTotal");
      this.physicalTotal = physicalTotal;
      initBits &= ~INIT_BIT_PHYSICAL_TOTAL;
      return this;
    }

    /**
     * Initializes the value for the {@link SystemMemoryOshi#getSwapTotal() swapTotal} attribute.
     * @param swapTotal The value for swapTotal 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder swapTotal(long swapTotal) {
      checkNotIsSet(swapTotalIsSet(), "swapTotal");
      this.swapTotal = swapTotal;
      initBits &= ~INIT_BIT_SWAP_TOTAL;
      return this;
    }

    /**
     * Initializes the value for the {@link SystemMemoryOshi#getPageSize() pageSize} attribute.
     * @param pageSize The value for pageSize 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder pageSize(long pageSize) {
      checkNotIsSet(pageSizeIsSet(), "pageSize");
      this.pageSize = pageSize;
      initBits &= ~INIT_BIT_PAGE_SIZE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableSystemMemoryOshi ImmutableSystemMemoryOshi}.
     * @return An immutable instance of SystemMemoryOshi
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableSystemMemoryOshi build() {
      checkRequiredAttributes();
      return new ImmutableSystemMemoryOshi(this);
    }

    private boolean physicalTotalIsSet() {
      return (initBits & INIT_BIT_PHYSICAL_TOTAL) == 0;
    }

    private boolean swapTotalIsSet() {
      return (initBits & INIT_BIT_SWAP_TOTAL) == 0;
    }

    private boolean pageSizeIsSet() {
      return (initBits & INIT_BIT_PAGE_SIZE) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of SystemMemoryOshi is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!physicalTotalIsSet()) attributes.add("physicalTotal");
      if (!swapTotalIsSet()) attributes.add("swapTotal");
      if (!pageSizeIsSet()) attributes.add("pageSize");
      return "Cannot build SystemMemoryOshi, some of required attributes are not set " + attributes;
    }
  }
}
