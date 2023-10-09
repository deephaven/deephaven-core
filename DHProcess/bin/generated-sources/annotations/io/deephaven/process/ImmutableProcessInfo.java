package io.deephaven.process;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ProcessInfo}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableProcessInfo.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableProcessInfo.of()}.
 */
@Generated(from = "ProcessInfo", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableProcessInfo extends ProcessInfo {
  private final ProcessUniqueId id;
  private final RuntimeMxBeanInfo runtimeInfo;
  private final EnvironmentVariables environmentVariables;
  private final ThreadMxBeanInfo threadInfo;
  private final MemoryMxBeanInfo memoryInfo;
  private final MemoryPoolsMxBeanInfo memoryPoolsInfo;
  private final ApplicationArguments applicationArguments;
  private final ApplicationConfig applicationConfig;
  private final HostPathInfo hostPathInfo;
  private final SystemInfoOshi systemInfo;

  @SuppressWarnings("unchecked") // safe covariant cast
  private ImmutableProcessInfo(
      ProcessUniqueId id,
      RuntimeMxBeanInfo runtimeInfo,
      EnvironmentVariables environmentVariables,
      ThreadMxBeanInfo threadInfo,
      MemoryMxBeanInfo memoryInfo,
      MemoryPoolsMxBeanInfo memoryPoolsInfo,
      ApplicationArguments applicationArguments,
      ApplicationConfig applicationConfig,
      HostPathInfo hostPathInfo,
      Optional<? extends SystemInfoOshi> systemInfo) {
    this.id = Objects.requireNonNull(id, "id");
    this.runtimeInfo = Objects.requireNonNull(runtimeInfo, "runtimeInfo");
    this.environmentVariables = Objects.requireNonNull(environmentVariables, "environmentVariables");
    this.threadInfo = Objects.requireNonNull(threadInfo, "threadInfo");
    this.memoryInfo = Objects.requireNonNull(memoryInfo, "memoryInfo");
    this.memoryPoolsInfo = Objects.requireNonNull(memoryPoolsInfo, "memoryPoolsInfo");
    this.applicationArguments = Objects.requireNonNull(applicationArguments, "applicationArguments");
    this.applicationConfig = Objects.requireNonNull(applicationConfig, "applicationConfig");
    this.hostPathInfo = Objects.requireNonNull(hostPathInfo, "hostPathInfo");
    this.systemInfo = systemInfo.orElse(null);
  }

  private ImmutableProcessInfo(ImmutableProcessInfo.Builder builder) {
    this.id = builder.id;
    this.runtimeInfo = builder.runtimeInfo;
    this.environmentVariables = builder.environmentVariables;
    this.threadInfo = builder.threadInfo;
    this.memoryInfo = builder.memoryInfo;
    this.memoryPoolsInfo = builder.memoryPoolsInfo;
    this.applicationArguments = builder.applicationArguments;
    this.applicationConfig = builder.applicationConfig;
    this.hostPathInfo = builder.hostPathInfo;
    this.systemInfo = builder.systemInfo;
  }

  /**
   * @return The value of the {@code id} attribute
   */
  @Override
  public ProcessUniqueId getId() {
    return id;
  }

  /**
   * @return The value of the {@code runtimeInfo} attribute
   */
  @Override
  public RuntimeMxBeanInfo getRuntimeInfo() {
    return runtimeInfo;
  }

  /**
   * @return The value of the {@code environmentVariables} attribute
   */
  @Override
  public EnvironmentVariables getEnvironmentVariables() {
    return environmentVariables;
  }

  /**
   * @return The value of the {@code threadInfo} attribute
   */
  @Override
  public ThreadMxBeanInfo getThreadInfo() {
    return threadInfo;
  }

  /**
   * @return The value of the {@code memoryInfo} attribute
   */
  @Override
  public MemoryMxBeanInfo getMemoryInfo() {
    return memoryInfo;
  }

  /**
   * @return The value of the {@code memoryPoolsInfo} attribute
   */
  @Override
  public MemoryPoolsMxBeanInfo getMemoryPoolsInfo() {
    return memoryPoolsInfo;
  }

  /**
   * @return The value of the {@code applicationArguments} attribute
   */
  @Override
  public ApplicationArguments getApplicationArguments() {
    return applicationArguments;
  }

  /**
   * @return The value of the {@code applicationConfig} attribute
   */
  @Override
  public ApplicationConfig getApplicationConfig() {
    return applicationConfig;
  }

  /**
   * @return The value of the {@code hostPathInfo} attribute
   */
  @Override
  public HostPathInfo getHostPathInfo() {
    return hostPathInfo;
  }

  /**
   * @return The value of the {@code systemInfo} attribute
   */
  @Override
  public Optional<SystemInfoOshi> getSystemInfo() {
    return Optional.ofNullable(systemInfo);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableProcessInfo} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableProcessInfo
        && equalTo((ImmutableProcessInfo) another);
  }

  private boolean equalTo(ImmutableProcessInfo another) {
    return id.equals(another.id)
        && runtimeInfo.equals(another.runtimeInfo)
        && environmentVariables.equals(another.environmentVariables)
        && threadInfo.equals(another.threadInfo)
        && memoryInfo.equals(another.memoryInfo)
        && memoryPoolsInfo.equals(another.memoryPoolsInfo)
        && applicationArguments.equals(another.applicationArguments)
        && applicationConfig.equals(another.applicationConfig)
        && hostPathInfo.equals(another.hostPathInfo)
        && Objects.equals(systemInfo, another.systemInfo);
  }

  /**
   * Computes a hash code from attributes: {@code id}, {@code runtimeInfo}, {@code environmentVariables}, {@code threadInfo}, {@code memoryInfo}, {@code memoryPoolsInfo}, {@code applicationArguments}, {@code applicationConfig}, {@code hostPathInfo}, {@code systemInfo}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + id.hashCode();
    h += (h << 5) + runtimeInfo.hashCode();
    h += (h << 5) + environmentVariables.hashCode();
    h += (h << 5) + threadInfo.hashCode();
    h += (h << 5) + memoryInfo.hashCode();
    h += (h << 5) + memoryPoolsInfo.hashCode();
    h += (h << 5) + applicationArguments.hashCode();
    h += (h << 5) + applicationConfig.hashCode();
    h += (h << 5) + hostPathInfo.hashCode();
    h += (h << 5) + Objects.hashCode(systemInfo);
    return h;
  }


  /**
   * Prints the immutable value {@code ProcessInfo} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("ProcessInfo{");
    builder.append("id=").append(id);
    builder.append(", ");
    builder.append("runtimeInfo=").append(runtimeInfo);
    builder.append(", ");
    builder.append("environmentVariables=").append(environmentVariables);
    builder.append(", ");
    builder.append("threadInfo=").append(threadInfo);
    builder.append(", ");
    builder.append("memoryInfo=").append(memoryInfo);
    builder.append(", ");
    builder.append("memoryPoolsInfo=").append(memoryPoolsInfo);
    builder.append(", ");
    builder.append("applicationArguments=").append(applicationArguments);
    builder.append(", ");
    builder.append("applicationConfig=").append(applicationConfig);
    builder.append(", ");
    builder.append("hostPathInfo=").append(hostPathInfo);
    if (systemInfo != null) {
      builder.append(", ");
      builder.append("systemInfo=").append(systemInfo);
    }
    return builder.append("}").toString();
  }

  /**
   * Construct a new immutable {@code ProcessInfo} instance.
   * @param id The value for the {@code id} attribute
   * @param runtimeInfo The value for the {@code runtimeInfo} attribute
   * @param environmentVariables The value for the {@code environmentVariables} attribute
   * @param threadInfo The value for the {@code threadInfo} attribute
   * @param memoryInfo The value for the {@code memoryInfo} attribute
   * @param memoryPoolsInfo The value for the {@code memoryPoolsInfo} attribute
   * @param applicationArguments The value for the {@code applicationArguments} attribute
   * @param applicationConfig The value for the {@code applicationConfig} attribute
   * @param hostPathInfo The value for the {@code hostPathInfo} attribute
   * @param systemInfo The value for the {@code systemInfo} attribute
   * @return An immutable ProcessInfo instance
   */
  public static ImmutableProcessInfo of(ProcessUniqueId id, RuntimeMxBeanInfo runtimeInfo, EnvironmentVariables environmentVariables, ThreadMxBeanInfo threadInfo, MemoryMxBeanInfo memoryInfo, MemoryPoolsMxBeanInfo memoryPoolsInfo, ApplicationArguments applicationArguments, ApplicationConfig applicationConfig, HostPathInfo hostPathInfo, Optional<? extends SystemInfoOshi> systemInfo) {
    return new ImmutableProcessInfo(id, runtimeInfo, environmentVariables, threadInfo, memoryInfo, memoryPoolsInfo, applicationArguments, applicationConfig, hostPathInfo, systemInfo);
  }

  /**
   * Creates a builder for {@link ImmutableProcessInfo ImmutableProcessInfo}.
   * <pre>
   * ImmutableProcessInfo.builder()
   *    .id(io.deephaven.process.ProcessUniqueId) // required {@link ProcessInfo#getId() id}
   *    .runtimeInfo(io.deephaven.process.RuntimeMxBeanInfo) // required {@link ProcessInfo#getRuntimeInfo() runtimeInfo}
   *    .environmentVariables(io.deephaven.process.EnvironmentVariables) // required {@link ProcessInfo#getEnvironmentVariables() environmentVariables}
   *    .threadInfo(io.deephaven.process.ThreadMxBeanInfo) // required {@link ProcessInfo#getThreadInfo() threadInfo}
   *    .memoryInfo(io.deephaven.process.MemoryMxBeanInfo) // required {@link ProcessInfo#getMemoryInfo() memoryInfo}
   *    .memoryPoolsInfo(io.deephaven.process.MemoryPoolsMxBeanInfo) // required {@link ProcessInfo#getMemoryPoolsInfo() memoryPoolsInfo}
   *    .applicationArguments(io.deephaven.process.ApplicationArguments) // required {@link ProcessInfo#getApplicationArguments() applicationArguments}
   *    .applicationConfig(io.deephaven.process.ApplicationConfig) // required {@link ProcessInfo#getApplicationConfig() applicationConfig}
   *    .hostPathInfo(io.deephaven.process.HostPathInfo) // required {@link ProcessInfo#getHostPathInfo() hostPathInfo}
   *    .systemInfo(io.deephaven.process.SystemInfoOshi) // optional {@link ProcessInfo#getSystemInfo() systemInfo}
   *    .build();
   * </pre>
   * @return A new ImmutableProcessInfo builder
   */
  public static ImmutableProcessInfo.Builder builder() {
    return new ImmutableProcessInfo.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableProcessInfo ImmutableProcessInfo}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ProcessInfo", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_ID = 0x1L;
    private static final long INIT_BIT_RUNTIME_INFO = 0x2L;
    private static final long INIT_BIT_ENVIRONMENT_VARIABLES = 0x4L;
    private static final long INIT_BIT_THREAD_INFO = 0x8L;
    private static final long INIT_BIT_MEMORY_INFO = 0x10L;
    private static final long INIT_BIT_MEMORY_POOLS_INFO = 0x20L;
    private static final long INIT_BIT_APPLICATION_ARGUMENTS = 0x40L;
    private static final long INIT_BIT_APPLICATION_CONFIG = 0x80L;
    private static final long INIT_BIT_HOST_PATH_INFO = 0x100L;
    private static final long OPT_BIT_SYSTEM_INFO = 0x1L;
    private long initBits = 0x1ffL;
    private long optBits;

    private ProcessUniqueId id;
    private RuntimeMxBeanInfo runtimeInfo;
    private EnvironmentVariables environmentVariables;
    private ThreadMxBeanInfo threadInfo;
    private MemoryMxBeanInfo memoryInfo;
    private MemoryPoolsMxBeanInfo memoryPoolsInfo;
    private ApplicationArguments applicationArguments;
    private ApplicationConfig applicationConfig;
    private HostPathInfo hostPathInfo;
    private SystemInfoOshi systemInfo;

    private Builder() {
    }

    /**
     * Initializes the value for the {@link ProcessInfo#getId() id} attribute.
     * @param id The value for id 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder id(ProcessUniqueId id) {
      checkNotIsSet(idIsSet(), "id");
      this.id = Objects.requireNonNull(id, "id");
      initBits &= ~INIT_BIT_ID;
      return this;
    }

    /**
     * Initializes the value for the {@link ProcessInfo#getRuntimeInfo() runtimeInfo} attribute.
     * @param runtimeInfo The value for runtimeInfo 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder runtimeInfo(RuntimeMxBeanInfo runtimeInfo) {
      checkNotIsSet(runtimeInfoIsSet(), "runtimeInfo");
      this.runtimeInfo = Objects.requireNonNull(runtimeInfo, "runtimeInfo");
      initBits &= ~INIT_BIT_RUNTIME_INFO;
      return this;
    }

    /**
     * Initializes the value for the {@link ProcessInfo#getEnvironmentVariables() environmentVariables} attribute.
     * @param environmentVariables The value for environmentVariables 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder environmentVariables(EnvironmentVariables environmentVariables) {
      checkNotIsSet(environmentVariablesIsSet(), "environmentVariables");
      this.environmentVariables = Objects.requireNonNull(environmentVariables, "environmentVariables");
      initBits &= ~INIT_BIT_ENVIRONMENT_VARIABLES;
      return this;
    }

    /**
     * Initializes the value for the {@link ProcessInfo#getThreadInfo() threadInfo} attribute.
     * @param threadInfo The value for threadInfo 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder threadInfo(ThreadMxBeanInfo threadInfo) {
      checkNotIsSet(threadInfoIsSet(), "threadInfo");
      this.threadInfo = Objects.requireNonNull(threadInfo, "threadInfo");
      initBits &= ~INIT_BIT_THREAD_INFO;
      return this;
    }

    /**
     * Initializes the value for the {@link ProcessInfo#getMemoryInfo() memoryInfo} attribute.
     * @param memoryInfo The value for memoryInfo 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder memoryInfo(MemoryMxBeanInfo memoryInfo) {
      checkNotIsSet(memoryInfoIsSet(), "memoryInfo");
      this.memoryInfo = Objects.requireNonNull(memoryInfo, "memoryInfo");
      initBits &= ~INIT_BIT_MEMORY_INFO;
      return this;
    }

    /**
     * Initializes the value for the {@link ProcessInfo#getMemoryPoolsInfo() memoryPoolsInfo} attribute.
     * @param memoryPoolsInfo The value for memoryPoolsInfo 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder memoryPoolsInfo(MemoryPoolsMxBeanInfo memoryPoolsInfo) {
      checkNotIsSet(memoryPoolsInfoIsSet(), "memoryPoolsInfo");
      this.memoryPoolsInfo = Objects.requireNonNull(memoryPoolsInfo, "memoryPoolsInfo");
      initBits &= ~INIT_BIT_MEMORY_POOLS_INFO;
      return this;
    }

    /**
     * Initializes the value for the {@link ProcessInfo#getApplicationArguments() applicationArguments} attribute.
     * @param applicationArguments The value for applicationArguments 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder applicationArguments(ApplicationArguments applicationArguments) {
      checkNotIsSet(applicationArgumentsIsSet(), "applicationArguments");
      this.applicationArguments = Objects.requireNonNull(applicationArguments, "applicationArguments");
      initBits &= ~INIT_BIT_APPLICATION_ARGUMENTS;
      return this;
    }

    /**
     * Initializes the value for the {@link ProcessInfo#getApplicationConfig() applicationConfig} attribute.
     * @param applicationConfig The value for applicationConfig 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder applicationConfig(ApplicationConfig applicationConfig) {
      checkNotIsSet(applicationConfigIsSet(), "applicationConfig");
      this.applicationConfig = Objects.requireNonNull(applicationConfig, "applicationConfig");
      initBits &= ~INIT_BIT_APPLICATION_CONFIG;
      return this;
    }

    /**
     * Initializes the value for the {@link ProcessInfo#getHostPathInfo() hostPathInfo} attribute.
     * @param hostPathInfo The value for hostPathInfo 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder hostPathInfo(HostPathInfo hostPathInfo) {
      checkNotIsSet(hostPathInfoIsSet(), "hostPathInfo");
      this.hostPathInfo = Objects.requireNonNull(hostPathInfo, "hostPathInfo");
      initBits &= ~INIT_BIT_HOST_PATH_INFO;
      return this;
    }

    /**
     * Initializes the optional value {@link ProcessInfo#getSystemInfo() systemInfo} to systemInfo.
     * @param systemInfo The value for systemInfo
     * @return {@code this} builder for chained invocation
     */
    public final Builder systemInfo(SystemInfoOshi systemInfo) {
      checkNotIsSet(systemInfoIsSet(), "systemInfo");
      this.systemInfo = Objects.requireNonNull(systemInfo, "systemInfo");
      optBits |= OPT_BIT_SYSTEM_INFO;
      return this;
    }

    /**
     * Initializes the optional value {@link ProcessInfo#getSystemInfo() systemInfo} to systemInfo.
     * @param systemInfo The value for systemInfo
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder systemInfo(Optional<? extends SystemInfoOshi> systemInfo) {
      checkNotIsSet(systemInfoIsSet(), "systemInfo");
      this.systemInfo = systemInfo.orElse(null);
      optBits |= OPT_BIT_SYSTEM_INFO;
      return this;
    }

    /**
     * Builds a new {@link ImmutableProcessInfo ImmutableProcessInfo}.
     * @return An immutable instance of ProcessInfo
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableProcessInfo build() {
      checkRequiredAttributes();
      return new ImmutableProcessInfo(this);
    }

    private boolean systemInfoIsSet() {
      return (optBits & OPT_BIT_SYSTEM_INFO) != 0;
    }

    private boolean idIsSet() {
      return (initBits & INIT_BIT_ID) == 0;
    }

    private boolean runtimeInfoIsSet() {
      return (initBits & INIT_BIT_RUNTIME_INFO) == 0;
    }

    private boolean environmentVariablesIsSet() {
      return (initBits & INIT_BIT_ENVIRONMENT_VARIABLES) == 0;
    }

    private boolean threadInfoIsSet() {
      return (initBits & INIT_BIT_THREAD_INFO) == 0;
    }

    private boolean memoryInfoIsSet() {
      return (initBits & INIT_BIT_MEMORY_INFO) == 0;
    }

    private boolean memoryPoolsInfoIsSet() {
      return (initBits & INIT_BIT_MEMORY_POOLS_INFO) == 0;
    }

    private boolean applicationArgumentsIsSet() {
      return (initBits & INIT_BIT_APPLICATION_ARGUMENTS) == 0;
    }

    private boolean applicationConfigIsSet() {
      return (initBits & INIT_BIT_APPLICATION_CONFIG) == 0;
    }

    private boolean hostPathInfoIsSet() {
      return (initBits & INIT_BIT_HOST_PATH_INFO) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of ProcessInfo is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!idIsSet()) attributes.add("id");
      if (!runtimeInfoIsSet()) attributes.add("runtimeInfo");
      if (!environmentVariablesIsSet()) attributes.add("environmentVariables");
      if (!threadInfoIsSet()) attributes.add("threadInfo");
      if (!memoryInfoIsSet()) attributes.add("memoryInfo");
      if (!memoryPoolsInfoIsSet()) attributes.add("memoryPoolsInfo");
      if (!applicationArgumentsIsSet()) attributes.add("applicationArguments");
      if (!applicationConfigIsSet()) attributes.add("applicationConfig");
      if (!hostPathInfoIsSet()) attributes.add("hostPathInfo");
      return "Cannot build ProcessInfo, some of required attributes are not set " + attributes;
    }
  }
}
