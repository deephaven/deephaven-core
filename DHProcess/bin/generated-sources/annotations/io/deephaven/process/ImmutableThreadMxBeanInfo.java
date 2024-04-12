package io.deephaven.process;

import java.util.ArrayList;
import java.util.List;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ThreadMxBeanInfo}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableThreadMxBeanInfo.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableThreadMxBeanInfo.of()}.
 */
@Generated(from = "ThreadMxBeanInfo", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableThreadMxBeanInfo extends ThreadMxBeanInfo {
  private final boolean isCurrentThreadCpuTimeSupported;
  private final boolean isObjectMonitorUsageSupported;
  private final boolean isSynchronizerUsageSupported;
  private final boolean isThreadContentionMonitoringSupported;
  private final boolean isThreadCpuTimeSupported;

  private ImmutableThreadMxBeanInfo(
      boolean isCurrentThreadCpuTimeSupported,
      boolean isObjectMonitorUsageSupported,
      boolean isSynchronizerUsageSupported,
      boolean isThreadContentionMonitoringSupported,
      boolean isThreadCpuTimeSupported) {
    this.isCurrentThreadCpuTimeSupported = isCurrentThreadCpuTimeSupported;
    this.isObjectMonitorUsageSupported = isObjectMonitorUsageSupported;
    this.isSynchronizerUsageSupported = isSynchronizerUsageSupported;
    this.isThreadContentionMonitoringSupported = isThreadContentionMonitoringSupported;
    this.isThreadCpuTimeSupported = isThreadCpuTimeSupported;
  }

  private ImmutableThreadMxBeanInfo(ImmutableThreadMxBeanInfo.Builder builder) {
    this.isCurrentThreadCpuTimeSupported = builder.isCurrentThreadCpuTimeSupported;
    this.isObjectMonitorUsageSupported = builder.isObjectMonitorUsageSupported;
    this.isSynchronizerUsageSupported = builder.isSynchronizerUsageSupported;
    this.isThreadContentionMonitoringSupported = builder.isThreadContentionMonitoringSupported;
    this.isThreadCpuTimeSupported = builder.isThreadCpuTimeSupported;
  }

  /**
   * @return The value of the {@code isCurrentThreadCpuTimeSupported} attribute
   */
  @Override
  public boolean isCurrentThreadCpuTimeSupported() {
    return isCurrentThreadCpuTimeSupported;
  }

  /**
   * @return The value of the {@code isObjectMonitorUsageSupported} attribute
   */
  @Override
  public boolean isObjectMonitorUsageSupported() {
    return isObjectMonitorUsageSupported;
  }

  /**
   * @return The value of the {@code isSynchronizerUsageSupported} attribute
   */
  @Override
  public boolean isSynchronizerUsageSupported() {
    return isSynchronizerUsageSupported;
  }

  /**
   * @return The value of the {@code isThreadContentionMonitoringSupported} attribute
   */
  @Override
  public boolean isThreadContentionMonitoringSupported() {
    return isThreadContentionMonitoringSupported;
  }

  /**
   * @return The value of the {@code isThreadCpuTimeSupported} attribute
   */
  @Override
  public boolean isThreadCpuTimeSupported() {
    return isThreadCpuTimeSupported;
  }

  /**
   * This instance is equal to all instances of {@code ImmutableThreadMxBeanInfo} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableThreadMxBeanInfo
        && equalTo((ImmutableThreadMxBeanInfo) another);
  }

  private boolean equalTo(ImmutableThreadMxBeanInfo another) {
    return isCurrentThreadCpuTimeSupported == another.isCurrentThreadCpuTimeSupported
        && isObjectMonitorUsageSupported == another.isObjectMonitorUsageSupported
        && isSynchronizerUsageSupported == another.isSynchronizerUsageSupported
        && isThreadContentionMonitoringSupported == another.isThreadContentionMonitoringSupported
        && isThreadCpuTimeSupported == another.isThreadCpuTimeSupported;
  }

  /**
   * Computes a hash code from attributes: {@code isCurrentThreadCpuTimeSupported}, {@code isObjectMonitorUsageSupported}, {@code isSynchronizerUsageSupported}, {@code isThreadContentionMonitoringSupported}, {@code isThreadCpuTimeSupported}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Boolean.hashCode(isCurrentThreadCpuTimeSupported);
    h += (h << 5) + Boolean.hashCode(isObjectMonitorUsageSupported);
    h += (h << 5) + Boolean.hashCode(isSynchronizerUsageSupported);
    h += (h << 5) + Boolean.hashCode(isThreadContentionMonitoringSupported);
    h += (h << 5) + Boolean.hashCode(isThreadCpuTimeSupported);
    return h;
  }


  /**
   * Prints the immutable value {@code ThreadMxBeanInfo} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ThreadMxBeanInfo{"
        + "isCurrentThreadCpuTimeSupported=" + isCurrentThreadCpuTimeSupported
        + ", isObjectMonitorUsageSupported=" + isObjectMonitorUsageSupported
        + ", isSynchronizerUsageSupported=" + isSynchronizerUsageSupported
        + ", isThreadContentionMonitoringSupported=" + isThreadContentionMonitoringSupported
        + ", isThreadCpuTimeSupported=" + isThreadCpuTimeSupported
        + "}";
  }

  /**
   * Construct a new immutable {@code ThreadMxBeanInfo} instance.
   * @param isCurrentThreadCpuTimeSupported The value for the {@code isCurrentThreadCpuTimeSupported} attribute
   * @param isObjectMonitorUsageSupported The value for the {@code isObjectMonitorUsageSupported} attribute
   * @param isSynchronizerUsageSupported The value for the {@code isSynchronizerUsageSupported} attribute
   * @param isThreadContentionMonitoringSupported The value for the {@code isThreadContentionMonitoringSupported} attribute
   * @param isThreadCpuTimeSupported The value for the {@code isThreadCpuTimeSupported} attribute
   * @return An immutable ThreadMxBeanInfo instance
   */
  public static ImmutableThreadMxBeanInfo of(boolean isCurrentThreadCpuTimeSupported, boolean isObjectMonitorUsageSupported, boolean isSynchronizerUsageSupported, boolean isThreadContentionMonitoringSupported, boolean isThreadCpuTimeSupported) {
    return new ImmutableThreadMxBeanInfo(isCurrentThreadCpuTimeSupported, isObjectMonitorUsageSupported, isSynchronizerUsageSupported, isThreadContentionMonitoringSupported, isThreadCpuTimeSupported);
  }

  /**
   * Creates a builder for {@link ImmutableThreadMxBeanInfo ImmutableThreadMxBeanInfo}.
   * <pre>
   * ImmutableThreadMxBeanInfo.builder()
   *    .isCurrentThreadCpuTimeSupported(boolean) // required {@link ThreadMxBeanInfo#isCurrentThreadCpuTimeSupported() isCurrentThreadCpuTimeSupported}
   *    .isObjectMonitorUsageSupported(boolean) // required {@link ThreadMxBeanInfo#isObjectMonitorUsageSupported() isObjectMonitorUsageSupported}
   *    .isSynchronizerUsageSupported(boolean) // required {@link ThreadMxBeanInfo#isSynchronizerUsageSupported() isSynchronizerUsageSupported}
   *    .isThreadContentionMonitoringSupported(boolean) // required {@link ThreadMxBeanInfo#isThreadContentionMonitoringSupported() isThreadContentionMonitoringSupported}
   *    .isThreadCpuTimeSupported(boolean) // required {@link ThreadMxBeanInfo#isThreadCpuTimeSupported() isThreadCpuTimeSupported}
   *    .build();
   * </pre>
   * @return A new ImmutableThreadMxBeanInfo builder
   */
  public static ImmutableThreadMxBeanInfo.Builder builder() {
    return new ImmutableThreadMxBeanInfo.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableThreadMxBeanInfo ImmutableThreadMxBeanInfo}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ThreadMxBeanInfo", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_IS_CURRENT_THREAD_CPU_TIME_SUPPORTED = 0x1L;
    private static final long INIT_BIT_IS_OBJECT_MONITOR_USAGE_SUPPORTED = 0x2L;
    private static final long INIT_BIT_IS_SYNCHRONIZER_USAGE_SUPPORTED = 0x4L;
    private static final long INIT_BIT_IS_THREAD_CONTENTION_MONITORING_SUPPORTED = 0x8L;
    private static final long INIT_BIT_IS_THREAD_CPU_TIME_SUPPORTED = 0x10L;
    private long initBits = 0x1fL;

    private boolean isCurrentThreadCpuTimeSupported;
    private boolean isObjectMonitorUsageSupported;
    private boolean isSynchronizerUsageSupported;
    private boolean isThreadContentionMonitoringSupported;
    private boolean isThreadCpuTimeSupported;

    private Builder() {
    }

    /**
     * Initializes the value for the {@link ThreadMxBeanInfo#isCurrentThreadCpuTimeSupported() isCurrentThreadCpuTimeSupported} attribute.
     * @param isCurrentThreadCpuTimeSupported The value for isCurrentThreadCpuTimeSupported 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder isCurrentThreadCpuTimeSupported(boolean isCurrentThreadCpuTimeSupported) {
      checkNotIsSet(isCurrentThreadCpuTimeSupportedIsSet(), "isCurrentThreadCpuTimeSupported");
      this.isCurrentThreadCpuTimeSupported = isCurrentThreadCpuTimeSupported;
      initBits &= ~INIT_BIT_IS_CURRENT_THREAD_CPU_TIME_SUPPORTED;
      return this;
    }

    /**
     * Initializes the value for the {@link ThreadMxBeanInfo#isObjectMonitorUsageSupported() isObjectMonitorUsageSupported} attribute.
     * @param isObjectMonitorUsageSupported The value for isObjectMonitorUsageSupported 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder isObjectMonitorUsageSupported(boolean isObjectMonitorUsageSupported) {
      checkNotIsSet(isObjectMonitorUsageSupportedIsSet(), "isObjectMonitorUsageSupported");
      this.isObjectMonitorUsageSupported = isObjectMonitorUsageSupported;
      initBits &= ~INIT_BIT_IS_OBJECT_MONITOR_USAGE_SUPPORTED;
      return this;
    }

    /**
     * Initializes the value for the {@link ThreadMxBeanInfo#isSynchronizerUsageSupported() isSynchronizerUsageSupported} attribute.
     * @param isSynchronizerUsageSupported The value for isSynchronizerUsageSupported 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder isSynchronizerUsageSupported(boolean isSynchronizerUsageSupported) {
      checkNotIsSet(isSynchronizerUsageSupportedIsSet(), "isSynchronizerUsageSupported");
      this.isSynchronizerUsageSupported = isSynchronizerUsageSupported;
      initBits &= ~INIT_BIT_IS_SYNCHRONIZER_USAGE_SUPPORTED;
      return this;
    }

    /**
     * Initializes the value for the {@link ThreadMxBeanInfo#isThreadContentionMonitoringSupported() isThreadContentionMonitoringSupported} attribute.
     * @param isThreadContentionMonitoringSupported The value for isThreadContentionMonitoringSupported 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder isThreadContentionMonitoringSupported(boolean isThreadContentionMonitoringSupported) {
      checkNotIsSet(isThreadContentionMonitoringSupportedIsSet(), "isThreadContentionMonitoringSupported");
      this.isThreadContentionMonitoringSupported = isThreadContentionMonitoringSupported;
      initBits &= ~INIT_BIT_IS_THREAD_CONTENTION_MONITORING_SUPPORTED;
      return this;
    }

    /**
     * Initializes the value for the {@link ThreadMxBeanInfo#isThreadCpuTimeSupported() isThreadCpuTimeSupported} attribute.
     * @param isThreadCpuTimeSupported The value for isThreadCpuTimeSupported 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder isThreadCpuTimeSupported(boolean isThreadCpuTimeSupported) {
      checkNotIsSet(isThreadCpuTimeSupportedIsSet(), "isThreadCpuTimeSupported");
      this.isThreadCpuTimeSupported = isThreadCpuTimeSupported;
      initBits &= ~INIT_BIT_IS_THREAD_CPU_TIME_SUPPORTED;
      return this;
    }

    /**
     * Builds a new {@link ImmutableThreadMxBeanInfo ImmutableThreadMxBeanInfo}.
     * @return An immutable instance of ThreadMxBeanInfo
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableThreadMxBeanInfo build() {
      checkRequiredAttributes();
      return new ImmutableThreadMxBeanInfo(this);
    }

    private boolean isCurrentThreadCpuTimeSupportedIsSet() {
      return (initBits & INIT_BIT_IS_CURRENT_THREAD_CPU_TIME_SUPPORTED) == 0;
    }

    private boolean isObjectMonitorUsageSupportedIsSet() {
      return (initBits & INIT_BIT_IS_OBJECT_MONITOR_USAGE_SUPPORTED) == 0;
    }

    private boolean isSynchronizerUsageSupportedIsSet() {
      return (initBits & INIT_BIT_IS_SYNCHRONIZER_USAGE_SUPPORTED) == 0;
    }

    private boolean isThreadContentionMonitoringSupportedIsSet() {
      return (initBits & INIT_BIT_IS_THREAD_CONTENTION_MONITORING_SUPPORTED) == 0;
    }

    private boolean isThreadCpuTimeSupportedIsSet() {
      return (initBits & INIT_BIT_IS_THREAD_CPU_TIME_SUPPORTED) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of ThreadMxBeanInfo is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!isCurrentThreadCpuTimeSupportedIsSet()) attributes.add("isCurrentThreadCpuTimeSupported");
      if (!isObjectMonitorUsageSupportedIsSet()) attributes.add("isObjectMonitorUsageSupported");
      if (!isSynchronizerUsageSupportedIsSet()) attributes.add("isSynchronizerUsageSupported");
      if (!isThreadContentionMonitoringSupportedIsSet()) attributes.add("isThreadContentionMonitoringSupported");
      if (!isThreadCpuTimeSupportedIsSet()) attributes.add("isThreadCpuTimeSupported");
      return "Cannot build ThreadMxBeanInfo, some of required attributes are not set " + attributes;
    }
  }
}
