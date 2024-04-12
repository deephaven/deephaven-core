package io.deephaven.process;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link RuntimeMxBeanInfo}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableRuntimeMxBeanInfo.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableRuntimeMxBeanInfo.of()}.
 */
@Generated(from = "RuntimeMxBeanInfo", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableRuntimeMxBeanInfo extends RuntimeMxBeanInfo {
  private final SystemProperties systemProperties;
  private final JvmArguments jvmArguments;
  private final String managementSpecVersion;
  private final boolean isBootClassPathSupported;
  private final long startTime;

  private ImmutableRuntimeMxBeanInfo(
      SystemProperties systemProperties,
      JvmArguments jvmArguments,
      String managementSpecVersion,
      boolean isBootClassPathSupported,
      long startTime) {
    this.systemProperties = Objects.requireNonNull(systemProperties, "systemProperties");
    this.jvmArguments = Objects.requireNonNull(jvmArguments, "jvmArguments");
    this.managementSpecVersion = Objects.requireNonNull(managementSpecVersion, "managementSpecVersion");
    this.isBootClassPathSupported = isBootClassPathSupported;
    this.startTime = startTime;
  }

  private ImmutableRuntimeMxBeanInfo(ImmutableRuntimeMxBeanInfo.Builder builder) {
    this.systemProperties = builder.systemProperties;
    this.jvmArguments = builder.jvmArguments;
    this.managementSpecVersion = builder.managementSpecVersion;
    this.isBootClassPathSupported = builder.isBootClassPathSupported;
    this.startTime = builder.startTime;
  }

  /**
   * @return The value of the {@code systemProperties} attribute
   */
  @Override
  public SystemProperties getSystemProperties() {
    return systemProperties;
  }

  /**
   * @return The value of the {@code jvmArguments} attribute
   */
  @Override
  public JvmArguments getJvmArguments() {
    return jvmArguments;
  }

  /**
   * @return The value of the {@code managementSpecVersion} attribute
   */
  @Override
  public String getManagementSpecVersion() {
    return managementSpecVersion;
  }

  /**
   * @return The value of the {@code isBootClassPathSupported} attribute
   */
  @Override
  public boolean isBootClassPathSupported() {
    return isBootClassPathSupported;
  }

  /**
   * @return The value of the {@code startTime} attribute
   */
  @Override
  public long getStartTime() {
    return startTime;
  }

  /**
   * This instance is equal to all instances of {@code ImmutableRuntimeMxBeanInfo} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableRuntimeMxBeanInfo
        && equalTo((ImmutableRuntimeMxBeanInfo) another);
  }

  private boolean equalTo(ImmutableRuntimeMxBeanInfo another) {
    return systemProperties.equals(another.systemProperties)
        && jvmArguments.equals(another.jvmArguments)
        && managementSpecVersion.equals(another.managementSpecVersion)
        && isBootClassPathSupported == another.isBootClassPathSupported
        && startTime == another.startTime;
  }

  /**
   * Computes a hash code from attributes: {@code systemProperties}, {@code jvmArguments}, {@code managementSpecVersion}, {@code isBootClassPathSupported}, {@code startTime}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + systemProperties.hashCode();
    h += (h << 5) + jvmArguments.hashCode();
    h += (h << 5) + managementSpecVersion.hashCode();
    h += (h << 5) + Boolean.hashCode(isBootClassPathSupported);
    h += (h << 5) + Long.hashCode(startTime);
    return h;
  }


  /**
   * Prints the immutable value {@code RuntimeMxBeanInfo} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "RuntimeMxBeanInfo{"
        + "systemProperties=" + systemProperties
        + ", jvmArguments=" + jvmArguments
        + ", managementSpecVersion=" + managementSpecVersion
        + ", isBootClassPathSupported=" + isBootClassPathSupported
        + ", startTime=" + startTime
        + "}";
  }

  /**
   * Construct a new immutable {@code RuntimeMxBeanInfo} instance.
   * @param systemProperties The value for the {@code systemProperties} attribute
   * @param jvmArguments The value for the {@code jvmArguments} attribute
   * @param managementSpecVersion The value for the {@code managementSpecVersion} attribute
   * @param isBootClassPathSupported The value for the {@code isBootClassPathSupported} attribute
   * @param startTime The value for the {@code startTime} attribute
   * @return An immutable RuntimeMxBeanInfo instance
   */
  public static ImmutableRuntimeMxBeanInfo of(SystemProperties systemProperties, JvmArguments jvmArguments, String managementSpecVersion, boolean isBootClassPathSupported, long startTime) {
    return new ImmutableRuntimeMxBeanInfo(systemProperties, jvmArguments, managementSpecVersion, isBootClassPathSupported, startTime);
  }

  /**
   * Creates a builder for {@link ImmutableRuntimeMxBeanInfo ImmutableRuntimeMxBeanInfo}.
   * <pre>
   * ImmutableRuntimeMxBeanInfo.builder()
   *    .systemProperties(io.deephaven.process.SystemProperties) // required {@link RuntimeMxBeanInfo#getSystemProperties() systemProperties}
   *    .jvmArguments(io.deephaven.process.JvmArguments) // required {@link RuntimeMxBeanInfo#getJvmArguments() jvmArguments}
   *    .managementSpecVersion(String) // required {@link RuntimeMxBeanInfo#getManagementSpecVersion() managementSpecVersion}
   *    .isBootClassPathSupported(boolean) // required {@link RuntimeMxBeanInfo#isBootClassPathSupported() isBootClassPathSupported}
   *    .startTime(long) // required {@link RuntimeMxBeanInfo#getStartTime() startTime}
   *    .build();
   * </pre>
   * @return A new ImmutableRuntimeMxBeanInfo builder
   */
  public static ImmutableRuntimeMxBeanInfo.Builder builder() {
    return new ImmutableRuntimeMxBeanInfo.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableRuntimeMxBeanInfo ImmutableRuntimeMxBeanInfo}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "RuntimeMxBeanInfo", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_SYSTEM_PROPERTIES = 0x1L;
    private static final long INIT_BIT_JVM_ARGUMENTS = 0x2L;
    private static final long INIT_BIT_MANAGEMENT_SPEC_VERSION = 0x4L;
    private static final long INIT_BIT_IS_BOOT_CLASS_PATH_SUPPORTED = 0x8L;
    private static final long INIT_BIT_START_TIME = 0x10L;
    private long initBits = 0x1fL;

    private SystemProperties systemProperties;
    private JvmArguments jvmArguments;
    private String managementSpecVersion;
    private boolean isBootClassPathSupported;
    private long startTime;

    private Builder() {
    }

    /**
     * Initializes the value for the {@link RuntimeMxBeanInfo#getSystemProperties() systemProperties} attribute.
     * @param systemProperties The value for systemProperties 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder systemProperties(SystemProperties systemProperties) {
      checkNotIsSet(systemPropertiesIsSet(), "systemProperties");
      this.systemProperties = Objects.requireNonNull(systemProperties, "systemProperties");
      initBits &= ~INIT_BIT_SYSTEM_PROPERTIES;
      return this;
    }

    /**
     * Initializes the value for the {@link RuntimeMxBeanInfo#getJvmArguments() jvmArguments} attribute.
     * @param jvmArguments The value for jvmArguments 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder jvmArguments(JvmArguments jvmArguments) {
      checkNotIsSet(jvmArgumentsIsSet(), "jvmArguments");
      this.jvmArguments = Objects.requireNonNull(jvmArguments, "jvmArguments");
      initBits &= ~INIT_BIT_JVM_ARGUMENTS;
      return this;
    }

    /**
     * Initializes the value for the {@link RuntimeMxBeanInfo#getManagementSpecVersion() managementSpecVersion} attribute.
     * @param managementSpecVersion The value for managementSpecVersion 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder managementSpecVersion(String managementSpecVersion) {
      checkNotIsSet(managementSpecVersionIsSet(), "managementSpecVersion");
      this.managementSpecVersion = Objects.requireNonNull(managementSpecVersion, "managementSpecVersion");
      initBits &= ~INIT_BIT_MANAGEMENT_SPEC_VERSION;
      return this;
    }

    /**
     * Initializes the value for the {@link RuntimeMxBeanInfo#isBootClassPathSupported() isBootClassPathSupported} attribute.
     * @param isBootClassPathSupported The value for isBootClassPathSupported 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder isBootClassPathSupported(boolean isBootClassPathSupported) {
      checkNotIsSet(isBootClassPathSupportedIsSet(), "isBootClassPathSupported");
      this.isBootClassPathSupported = isBootClassPathSupported;
      initBits &= ~INIT_BIT_IS_BOOT_CLASS_PATH_SUPPORTED;
      return this;
    }

    /**
     * Initializes the value for the {@link RuntimeMxBeanInfo#getStartTime() startTime} attribute.
     * @param startTime The value for startTime 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder startTime(long startTime) {
      checkNotIsSet(startTimeIsSet(), "startTime");
      this.startTime = startTime;
      initBits &= ~INIT_BIT_START_TIME;
      return this;
    }

    /**
     * Builds a new {@link ImmutableRuntimeMxBeanInfo ImmutableRuntimeMxBeanInfo}.
     * @return An immutable instance of RuntimeMxBeanInfo
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableRuntimeMxBeanInfo build() {
      checkRequiredAttributes();
      return new ImmutableRuntimeMxBeanInfo(this);
    }

    private boolean systemPropertiesIsSet() {
      return (initBits & INIT_BIT_SYSTEM_PROPERTIES) == 0;
    }

    private boolean jvmArgumentsIsSet() {
      return (initBits & INIT_BIT_JVM_ARGUMENTS) == 0;
    }

    private boolean managementSpecVersionIsSet() {
      return (initBits & INIT_BIT_MANAGEMENT_SPEC_VERSION) == 0;
    }

    private boolean isBootClassPathSupportedIsSet() {
      return (initBits & INIT_BIT_IS_BOOT_CLASS_PATH_SUPPORTED) == 0;
    }

    private boolean startTimeIsSet() {
      return (initBits & INIT_BIT_START_TIME) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of RuntimeMxBeanInfo is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!systemPropertiesIsSet()) attributes.add("systemProperties");
      if (!jvmArgumentsIsSet()) attributes.add("jvmArguments");
      if (!managementSpecVersionIsSet()) attributes.add("managementSpecVersion");
      if (!isBootClassPathSupportedIsSet()) attributes.add("isBootClassPathSupported");
      if (!startTimeIsSet()) attributes.add("startTime");
      return "Cannot build RuntimeMxBeanInfo, some of required attributes are not set " + attributes;
    }
  }
}
