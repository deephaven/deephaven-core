package io.deephaven.process;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link SystemCpuOshi}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableSystemCpuOshi.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableSystemCpuOshi.of()}.
 */
@Generated(from = "SystemCpuOshi", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableSystemCpuOshi extends SystemCpuOshi {
  private final String vendor;
  private final String name;
  private final Long vendorFreq;
  private final String processorID;
  private final String stepping;
  private final String model;
  private final String family;
  private final int logicalProcessorCount;
  private final int physicalProcessorCount;
  private final int physicalPackageCount;
  private final boolean is64bit;

  private ImmutableSystemCpuOshi(
      String vendor,
      String name,
      OptionalLong vendorFreq,
      String processorID,
      String stepping,
      String model,
      String family,
      int logicalProcessorCount,
      int physicalProcessorCount,
      int physicalPackageCount,
      boolean is64bit) {
    this.vendor = Objects.requireNonNull(vendor, "vendor");
    this.name = Objects.requireNonNull(name, "name");
    this.vendorFreq = vendorFreq.isPresent() ? vendorFreq.getAsLong() : null;
    this.processorID = Objects.requireNonNull(processorID, "processorID");
    this.stepping = Objects.requireNonNull(stepping, "stepping");
    this.model = Objects.requireNonNull(model, "model");
    this.family = Objects.requireNonNull(family, "family");
    this.logicalProcessorCount = logicalProcessorCount;
    this.physicalProcessorCount = physicalProcessorCount;
    this.physicalPackageCount = physicalPackageCount;
    this.is64bit = is64bit;
  }

  private ImmutableSystemCpuOshi(ImmutableSystemCpuOshi.Builder builder) {
    this.vendor = builder.vendor;
    this.name = builder.name;
    this.vendorFreq = builder.vendorFreq;
    this.processorID = builder.processorID;
    this.stepping = builder.stepping;
    this.model = builder.model;
    this.family = builder.family;
    this.logicalProcessorCount = builder.logicalProcessorCount;
    this.physicalProcessorCount = builder.physicalProcessorCount;
    this.physicalPackageCount = builder.physicalPackageCount;
    this.is64bit = builder.is64bit;
  }

  /**
   * @return The value of the {@code vendor} attribute
   */
  @Override
  public String getVendor() {
    return vendor;
  }

  /**
   * @return The value of the {@code name} attribute
   */
  @Override
  public String getName() {
    return name;
  }

  /**
   * @return The value of the {@code vendorFreq} attribute
   */
  @Override
  public OptionalLong getVendorFreq() {
    return vendorFreq != null
        ? OptionalLong.of(vendorFreq)
        : OptionalLong.empty();
  }

  /**
   * @return The value of the {@code processorID} attribute
   */
  @Override
  public String getProcessorID() {
    return processorID;
  }

  /**
   * @return The value of the {@code stepping} attribute
   */
  @Override
  public String getStepping() {
    return stepping;
  }

  /**
   * @return The value of the {@code model} attribute
   */
  @Override
  public String getModel() {
    return model;
  }

  /**
   * @return The value of the {@code family} attribute
   */
  @Override
  public String getFamily() {
    return family;
  }

  /**
   * @return The value of the {@code logicalProcessorCount} attribute
   */
  @Override
  public int getLogicalProcessorCount() {
    return logicalProcessorCount;
  }

  /**
   * @return The value of the {@code physicalProcessorCount} attribute
   */
  @Override
  public int getPhysicalProcessorCount() {
    return physicalProcessorCount;
  }

  /**
   * @return The value of the {@code physicalPackageCount} attribute
   */
  @Override
  public int getPhysicalPackageCount() {
    return physicalPackageCount;
  }

  /**
   * @return The value of the {@code is64bit} attribute
   */
  @Override
  public boolean is64bit() {
    return is64bit;
  }

  /**
   * This instance is equal to all instances of {@code ImmutableSystemCpuOshi} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableSystemCpuOshi
        && equalTo((ImmutableSystemCpuOshi) another);
  }

  private boolean equalTo(ImmutableSystemCpuOshi another) {
    return vendor.equals(another.vendor)
        && name.equals(another.name)
        && Objects.equals(vendorFreq, another.vendorFreq)
        && processorID.equals(another.processorID)
        && stepping.equals(another.stepping)
        && model.equals(another.model)
        && family.equals(another.family)
        && logicalProcessorCount == another.logicalProcessorCount
        && physicalProcessorCount == another.physicalProcessorCount
        && physicalPackageCount == another.physicalPackageCount
        && is64bit == another.is64bit;
  }

  /**
   * Computes a hash code from attributes: {@code vendor}, {@code name}, {@code vendorFreq}, {@code processorID}, {@code stepping}, {@code model}, {@code family}, {@code logicalProcessorCount}, {@code physicalProcessorCount}, {@code physicalPackageCount}, {@code is64bit}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + vendor.hashCode();
    h += (h << 5) + name.hashCode();
    h += (h << 5) + Objects.hashCode(vendorFreq);
    h += (h << 5) + processorID.hashCode();
    h += (h << 5) + stepping.hashCode();
    h += (h << 5) + model.hashCode();
    h += (h << 5) + family.hashCode();
    h += (h << 5) + logicalProcessorCount;
    h += (h << 5) + physicalProcessorCount;
    h += (h << 5) + physicalPackageCount;
    h += (h << 5) + Boolean.hashCode(is64bit);
    return h;
  }


  /**
   * Prints the immutable value {@code SystemCpuOshi} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("SystemCpuOshi{");
    builder.append("vendor=").append(vendor);
    builder.append(", ");
    builder.append("name=").append(name);
    if (vendorFreq != null) {
      builder.append(", ");
      builder.append("vendorFreq=").append(vendorFreq);
    }
    builder.append(", ");
    builder.append("processorID=").append(processorID);
    builder.append(", ");
    builder.append("stepping=").append(stepping);
    builder.append(", ");
    builder.append("model=").append(model);
    builder.append(", ");
    builder.append("family=").append(family);
    builder.append(", ");
    builder.append("logicalProcessorCount=").append(logicalProcessorCount);
    builder.append(", ");
    builder.append("physicalProcessorCount=").append(physicalProcessorCount);
    builder.append(", ");
    builder.append("physicalPackageCount=").append(physicalPackageCount);
    builder.append(", ");
    builder.append("is64bit=").append(is64bit);
    return builder.append("}").toString();
  }

  /**
   * Construct a new immutable {@code SystemCpuOshi} instance.
   * @param vendor The value for the {@code vendor} attribute
   * @param name The value for the {@code name} attribute
   * @param vendorFreq The value for the {@code vendorFreq} attribute
   * @param processorID The value for the {@code processorID} attribute
   * @param stepping The value for the {@code stepping} attribute
   * @param model The value for the {@code model} attribute
   * @param family The value for the {@code family} attribute
   * @param logicalProcessorCount The value for the {@code logicalProcessorCount} attribute
   * @param physicalProcessorCount The value for the {@code physicalProcessorCount} attribute
   * @param physicalPackageCount The value for the {@code physicalPackageCount} attribute
   * @param is64bit The value for the {@code is64bit} attribute
   * @return An immutable SystemCpuOshi instance
   */
  public static ImmutableSystemCpuOshi of(String vendor, String name, OptionalLong vendorFreq, String processorID, String stepping, String model, String family, int logicalProcessorCount, int physicalProcessorCount, int physicalPackageCount, boolean is64bit) {
    return new ImmutableSystemCpuOshi(vendor, name, vendorFreq, processorID, stepping, model, family, logicalProcessorCount, physicalProcessorCount, physicalPackageCount, is64bit);
  }

  /**
   * Creates a builder for {@link ImmutableSystemCpuOshi ImmutableSystemCpuOshi}.
   * <pre>
   * ImmutableSystemCpuOshi.builder()
   *    .vendor(String) // required {@link SystemCpuOshi#getVendor() vendor}
   *    .name(String) // required {@link SystemCpuOshi#getName() name}
   *    .vendorFreq(long) // optional {@link SystemCpuOshi#getVendorFreq() vendorFreq}
   *    .processorID(String) // required {@link SystemCpuOshi#getProcessorID() processorID}
   *    .stepping(String) // required {@link SystemCpuOshi#getStepping() stepping}
   *    .model(String) // required {@link SystemCpuOshi#getModel() model}
   *    .family(String) // required {@link SystemCpuOshi#getFamily() family}
   *    .logicalProcessorCount(int) // required {@link SystemCpuOshi#getLogicalProcessorCount() logicalProcessorCount}
   *    .physicalProcessorCount(int) // required {@link SystemCpuOshi#getPhysicalProcessorCount() physicalProcessorCount}
   *    .physicalPackageCount(int) // required {@link SystemCpuOshi#getPhysicalPackageCount() physicalPackageCount}
   *    .is64bit(boolean) // required {@link SystemCpuOshi#is64bit() is64bit}
   *    .build();
   * </pre>
   * @return A new ImmutableSystemCpuOshi builder
   */
  public static ImmutableSystemCpuOshi.Builder builder() {
    return new ImmutableSystemCpuOshi.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableSystemCpuOshi ImmutableSystemCpuOshi}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "SystemCpuOshi", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_VENDOR = 0x1L;
    private static final long INIT_BIT_NAME = 0x2L;
    private static final long INIT_BIT_PROCESSOR_I_D = 0x4L;
    private static final long INIT_BIT_STEPPING = 0x8L;
    private static final long INIT_BIT_MODEL = 0x10L;
    private static final long INIT_BIT_FAMILY = 0x20L;
    private static final long INIT_BIT_LOGICAL_PROCESSOR_COUNT = 0x40L;
    private static final long INIT_BIT_PHYSICAL_PROCESSOR_COUNT = 0x80L;
    private static final long INIT_BIT_PHYSICAL_PACKAGE_COUNT = 0x100L;
    private static final long INIT_BIT_IS64BIT = 0x200L;
    private static final long OPT_BIT_VENDOR_FREQ = 0x1L;
    private long initBits = 0x3ffL;
    private long optBits;

    private String vendor;
    private String name;
    private Long vendorFreq;
    private String processorID;
    private String stepping;
    private String model;
    private String family;
    private int logicalProcessorCount;
    private int physicalProcessorCount;
    private int physicalPackageCount;
    private boolean is64bit;

    private Builder() {
    }

    /**
     * Initializes the value for the {@link SystemCpuOshi#getVendor() vendor} attribute.
     * @param vendor The value for vendor 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder vendor(String vendor) {
      checkNotIsSet(vendorIsSet(), "vendor");
      this.vendor = Objects.requireNonNull(vendor, "vendor");
      initBits &= ~INIT_BIT_VENDOR;
      return this;
    }

    /**
     * Initializes the value for the {@link SystemCpuOshi#getName() name} attribute.
     * @param name The value for name 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder name(String name) {
      checkNotIsSet(nameIsSet(), "name");
      this.name = Objects.requireNonNull(name, "name");
      initBits &= ~INIT_BIT_NAME;
      return this;
    }

    /**
     * Initializes the optional value {@link SystemCpuOshi#getVendorFreq() vendorFreq} to vendorFreq.
     * @param vendorFreq The value for vendorFreq
     * @return {@code this} builder for chained invocation
     */
    public final Builder vendorFreq(long vendorFreq) {
      checkNotIsSet(vendorFreqIsSet(), "vendorFreq");
      this.vendorFreq = vendorFreq;
      optBits |= OPT_BIT_VENDOR_FREQ;
      return this;
    }

    /**
     * Initializes the optional value {@link SystemCpuOshi#getVendorFreq() vendorFreq} to vendorFreq.
     * @param vendorFreq The value for vendorFreq
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder vendorFreq(OptionalLong vendorFreq) {
      checkNotIsSet(vendorFreqIsSet(), "vendorFreq");
      this.vendorFreq = vendorFreq.isPresent() ? vendorFreq.getAsLong() : null;
      optBits |= OPT_BIT_VENDOR_FREQ;
      return this;
    }

    /**
     * Initializes the value for the {@link SystemCpuOshi#getProcessorID() processorID} attribute.
     * @param processorID The value for processorID 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder processorID(String processorID) {
      checkNotIsSet(processorIDIsSet(), "processorID");
      this.processorID = Objects.requireNonNull(processorID, "processorID");
      initBits &= ~INIT_BIT_PROCESSOR_I_D;
      return this;
    }

    /**
     * Initializes the value for the {@link SystemCpuOshi#getStepping() stepping} attribute.
     * @param stepping The value for stepping 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder stepping(String stepping) {
      checkNotIsSet(steppingIsSet(), "stepping");
      this.stepping = Objects.requireNonNull(stepping, "stepping");
      initBits &= ~INIT_BIT_STEPPING;
      return this;
    }

    /**
     * Initializes the value for the {@link SystemCpuOshi#getModel() model} attribute.
     * @param model The value for model 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder model(String model) {
      checkNotIsSet(modelIsSet(), "model");
      this.model = Objects.requireNonNull(model, "model");
      initBits &= ~INIT_BIT_MODEL;
      return this;
    }

    /**
     * Initializes the value for the {@link SystemCpuOshi#getFamily() family} attribute.
     * @param family The value for family 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder family(String family) {
      checkNotIsSet(familyIsSet(), "family");
      this.family = Objects.requireNonNull(family, "family");
      initBits &= ~INIT_BIT_FAMILY;
      return this;
    }

    /**
     * Initializes the value for the {@link SystemCpuOshi#getLogicalProcessorCount() logicalProcessorCount} attribute.
     * @param logicalProcessorCount The value for logicalProcessorCount 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder logicalProcessorCount(int logicalProcessorCount) {
      checkNotIsSet(logicalProcessorCountIsSet(), "logicalProcessorCount");
      this.logicalProcessorCount = logicalProcessorCount;
      initBits &= ~INIT_BIT_LOGICAL_PROCESSOR_COUNT;
      return this;
    }

    /**
     * Initializes the value for the {@link SystemCpuOshi#getPhysicalProcessorCount() physicalProcessorCount} attribute.
     * @param physicalProcessorCount The value for physicalProcessorCount 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder physicalProcessorCount(int physicalProcessorCount) {
      checkNotIsSet(physicalProcessorCountIsSet(), "physicalProcessorCount");
      this.physicalProcessorCount = physicalProcessorCount;
      initBits &= ~INIT_BIT_PHYSICAL_PROCESSOR_COUNT;
      return this;
    }

    /**
     * Initializes the value for the {@link SystemCpuOshi#getPhysicalPackageCount() physicalPackageCount} attribute.
     * @param physicalPackageCount The value for physicalPackageCount 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder physicalPackageCount(int physicalPackageCount) {
      checkNotIsSet(physicalPackageCountIsSet(), "physicalPackageCount");
      this.physicalPackageCount = physicalPackageCount;
      initBits &= ~INIT_BIT_PHYSICAL_PACKAGE_COUNT;
      return this;
    }

    /**
     * Initializes the value for the {@link SystemCpuOshi#is64bit() is64bit} attribute.
     * @param is64bit The value for is64bit 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder is64bit(boolean is64bit) {
      checkNotIsSet(is64bitIsSet(), "is64bit");
      this.is64bit = is64bit;
      initBits &= ~INIT_BIT_IS64BIT;
      return this;
    }

    /**
     * Builds a new {@link ImmutableSystemCpuOshi ImmutableSystemCpuOshi}.
     * @return An immutable instance of SystemCpuOshi
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableSystemCpuOshi build() {
      checkRequiredAttributes();
      return new ImmutableSystemCpuOshi(this);
    }

    private boolean vendorFreqIsSet() {
      return (optBits & OPT_BIT_VENDOR_FREQ) != 0;
    }

    private boolean vendorIsSet() {
      return (initBits & INIT_BIT_VENDOR) == 0;
    }

    private boolean nameIsSet() {
      return (initBits & INIT_BIT_NAME) == 0;
    }

    private boolean processorIDIsSet() {
      return (initBits & INIT_BIT_PROCESSOR_I_D) == 0;
    }

    private boolean steppingIsSet() {
      return (initBits & INIT_BIT_STEPPING) == 0;
    }

    private boolean modelIsSet() {
      return (initBits & INIT_BIT_MODEL) == 0;
    }

    private boolean familyIsSet() {
      return (initBits & INIT_BIT_FAMILY) == 0;
    }

    private boolean logicalProcessorCountIsSet() {
      return (initBits & INIT_BIT_LOGICAL_PROCESSOR_COUNT) == 0;
    }

    private boolean physicalProcessorCountIsSet() {
      return (initBits & INIT_BIT_PHYSICAL_PROCESSOR_COUNT) == 0;
    }

    private boolean physicalPackageCountIsSet() {
      return (initBits & INIT_BIT_PHYSICAL_PACKAGE_COUNT) == 0;
    }

    private boolean is64bitIsSet() {
      return (initBits & INIT_BIT_IS64BIT) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of SystemCpuOshi is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!vendorIsSet()) attributes.add("vendor");
      if (!nameIsSet()) attributes.add("name");
      if (!processorIDIsSet()) attributes.add("processorID");
      if (!steppingIsSet()) attributes.add("stepping");
      if (!modelIsSet()) attributes.add("model");
      if (!familyIsSet()) attributes.add("family");
      if (!logicalProcessorCountIsSet()) attributes.add("logicalProcessorCount");
      if (!physicalProcessorCountIsSet()) attributes.add("physicalProcessorCount");
      if (!physicalPackageCountIsSet()) attributes.add("physicalPackageCount");
      if (!is64bitIsSet()) attributes.add("is64bit");
      return "Cannot build SystemCpuOshi, some of required attributes are not set " + attributes;
    }
  }
}
