package io.deephaven.process;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link SystemInfoOshi}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableSystemInfoOshi.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableSystemInfoOshi.of()}.
 */
@Generated(from = "SystemInfoOshi", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableSystemInfoOshi extends SystemInfoOshi {
  private final OperatingSystemOshi operatingSystem;
  private final ComputerSystemOshi computerSystem;
  private final SystemMemoryOshi systemMemory;
  private final SystemCpuOshi systemCpu;

  private ImmutableSystemInfoOshi(
      OperatingSystemOshi operatingSystem,
      ComputerSystemOshi computerSystem,
      SystemMemoryOshi systemMemory,
      SystemCpuOshi systemCpu) {
    this.operatingSystem = Objects.requireNonNull(operatingSystem, "operatingSystem");
    this.computerSystem = Objects.requireNonNull(computerSystem, "computerSystem");
    this.systemMemory = Objects.requireNonNull(systemMemory, "systemMemory");
    this.systemCpu = Objects.requireNonNull(systemCpu, "systemCpu");
  }

  private ImmutableSystemInfoOshi(ImmutableSystemInfoOshi.Builder builder) {
    this.operatingSystem = builder.operatingSystem;
    this.computerSystem = builder.computerSystem;
    this.systemMemory = builder.systemMemory;
    this.systemCpu = builder.systemCpu;
  }

  /**
   * @return The value of the {@code operatingSystem} attribute
   */
  @Override
  public OperatingSystemOshi getOperatingSystem() {
    return operatingSystem;
  }

  /**
   * @return The value of the {@code computerSystem} attribute
   */
  @Override
  public ComputerSystemOshi getComputerSystem() {
    return computerSystem;
  }

  /**
   * @return The value of the {@code systemMemory} attribute
   */
  @Override
  public SystemMemoryOshi getSystemMemory() {
    return systemMemory;
  }

  /**
   * @return The value of the {@code systemCpu} attribute
   */
  @Override
  public SystemCpuOshi getSystemCpu() {
    return systemCpu;
  }

  /**
   * This instance is equal to all instances of {@code ImmutableSystemInfoOshi} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableSystemInfoOshi
        && equalTo((ImmutableSystemInfoOshi) another);
  }

  private boolean equalTo(ImmutableSystemInfoOshi another) {
    return operatingSystem.equals(another.operatingSystem)
        && computerSystem.equals(another.computerSystem)
        && systemMemory.equals(another.systemMemory)
        && systemCpu.equals(another.systemCpu);
  }

  /**
   * Computes a hash code from attributes: {@code operatingSystem}, {@code computerSystem}, {@code systemMemory}, {@code systemCpu}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + operatingSystem.hashCode();
    h += (h << 5) + computerSystem.hashCode();
    h += (h << 5) + systemMemory.hashCode();
    h += (h << 5) + systemCpu.hashCode();
    return h;
  }


  /**
   * Prints the immutable value {@code SystemInfoOshi} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "SystemInfoOshi{"
        + "operatingSystem=" + operatingSystem
        + ", computerSystem=" + computerSystem
        + ", systemMemory=" + systemMemory
        + ", systemCpu=" + systemCpu
        + "}";
  }

  /**
   * Construct a new immutable {@code SystemInfoOshi} instance.
   * @param operatingSystem The value for the {@code operatingSystem} attribute
   * @param computerSystem The value for the {@code computerSystem} attribute
   * @param systemMemory The value for the {@code systemMemory} attribute
   * @param systemCpu The value for the {@code systemCpu} attribute
   * @return An immutable SystemInfoOshi instance
   */
  public static ImmutableSystemInfoOshi of(OperatingSystemOshi operatingSystem, ComputerSystemOshi computerSystem, SystemMemoryOshi systemMemory, SystemCpuOshi systemCpu) {
    return new ImmutableSystemInfoOshi(operatingSystem, computerSystem, systemMemory, systemCpu);
  }

  /**
   * Creates a builder for {@link ImmutableSystemInfoOshi ImmutableSystemInfoOshi}.
   * <pre>
   * ImmutableSystemInfoOshi.builder()
   *    .operatingSystem(io.deephaven.process.OperatingSystemOshi) // required {@link SystemInfoOshi#getOperatingSystem() operatingSystem}
   *    .computerSystem(io.deephaven.process.ComputerSystemOshi) // required {@link SystemInfoOshi#getComputerSystem() computerSystem}
   *    .systemMemory(io.deephaven.process.SystemMemoryOshi) // required {@link SystemInfoOshi#getSystemMemory() systemMemory}
   *    .systemCpu(io.deephaven.process.SystemCpuOshi) // required {@link SystemInfoOshi#getSystemCpu() systemCpu}
   *    .build();
   * </pre>
   * @return A new ImmutableSystemInfoOshi builder
   */
  public static ImmutableSystemInfoOshi.Builder builder() {
    return new ImmutableSystemInfoOshi.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableSystemInfoOshi ImmutableSystemInfoOshi}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "SystemInfoOshi", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_OPERATING_SYSTEM = 0x1L;
    private static final long INIT_BIT_COMPUTER_SYSTEM = 0x2L;
    private static final long INIT_BIT_SYSTEM_MEMORY = 0x4L;
    private static final long INIT_BIT_SYSTEM_CPU = 0x8L;
    private long initBits = 0xfL;

    private OperatingSystemOshi operatingSystem;
    private ComputerSystemOshi computerSystem;
    private SystemMemoryOshi systemMemory;
    private SystemCpuOshi systemCpu;

    private Builder() {
    }

    /**
     * Initializes the value for the {@link SystemInfoOshi#getOperatingSystem() operatingSystem} attribute.
     * @param operatingSystem The value for operatingSystem 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder operatingSystem(OperatingSystemOshi operatingSystem) {
      checkNotIsSet(operatingSystemIsSet(), "operatingSystem");
      this.operatingSystem = Objects.requireNonNull(operatingSystem, "operatingSystem");
      initBits &= ~INIT_BIT_OPERATING_SYSTEM;
      return this;
    }

    /**
     * Initializes the value for the {@link SystemInfoOshi#getComputerSystem() computerSystem} attribute.
     * @param computerSystem The value for computerSystem 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder computerSystem(ComputerSystemOshi computerSystem) {
      checkNotIsSet(computerSystemIsSet(), "computerSystem");
      this.computerSystem = Objects.requireNonNull(computerSystem, "computerSystem");
      initBits &= ~INIT_BIT_COMPUTER_SYSTEM;
      return this;
    }

    /**
     * Initializes the value for the {@link SystemInfoOshi#getSystemMemory() systemMemory} attribute.
     * @param systemMemory The value for systemMemory 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder systemMemory(SystemMemoryOshi systemMemory) {
      checkNotIsSet(systemMemoryIsSet(), "systemMemory");
      this.systemMemory = Objects.requireNonNull(systemMemory, "systemMemory");
      initBits &= ~INIT_BIT_SYSTEM_MEMORY;
      return this;
    }

    /**
     * Initializes the value for the {@link SystemInfoOshi#getSystemCpu() systemCpu} attribute.
     * @param systemCpu The value for systemCpu 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder systemCpu(SystemCpuOshi systemCpu) {
      checkNotIsSet(systemCpuIsSet(), "systemCpu");
      this.systemCpu = Objects.requireNonNull(systemCpu, "systemCpu");
      initBits &= ~INIT_BIT_SYSTEM_CPU;
      return this;
    }

    /**
     * Builds a new {@link ImmutableSystemInfoOshi ImmutableSystemInfoOshi}.
     * @return An immutable instance of SystemInfoOshi
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableSystemInfoOshi build() {
      checkRequiredAttributes();
      return new ImmutableSystemInfoOshi(this);
    }

    private boolean operatingSystemIsSet() {
      return (initBits & INIT_BIT_OPERATING_SYSTEM) == 0;
    }

    private boolean computerSystemIsSet() {
      return (initBits & INIT_BIT_COMPUTER_SYSTEM) == 0;
    }

    private boolean systemMemoryIsSet() {
      return (initBits & INIT_BIT_SYSTEM_MEMORY) == 0;
    }

    private boolean systemCpuIsSet() {
      return (initBits & INIT_BIT_SYSTEM_CPU) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of SystemInfoOshi is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!operatingSystemIsSet()) attributes.add("operatingSystem");
      if (!computerSystemIsSet()) attributes.add("computerSystem");
      if (!systemMemoryIsSet()) attributes.add("systemMemory");
      if (!systemCpuIsSet()) attributes.add("systemCpu");
      return "Cannot build SystemInfoOshi, some of required attributes are not set " + attributes;
    }
  }
}
