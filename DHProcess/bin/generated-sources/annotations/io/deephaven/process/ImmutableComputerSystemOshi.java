package io.deephaven.process;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ComputerSystemOshi}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableComputerSystemOshi.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableComputerSystemOshi.of()}.
 */
@Generated(from = "ComputerSystemOshi", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableComputerSystemOshi extends ComputerSystemOshi {
  private final String manufacturer;
  private final String model;
  private final FirmwareOshi firmware;
  private final BaseboardOshi baseboard;

  private ImmutableComputerSystemOshi(
      String manufacturer,
      String model,
      FirmwareOshi firmware,
      BaseboardOshi baseboard) {
    this.manufacturer = Objects.requireNonNull(manufacturer, "manufacturer");
    this.model = Objects.requireNonNull(model, "model");
    this.firmware = Objects.requireNonNull(firmware, "firmware");
    this.baseboard = Objects.requireNonNull(baseboard, "baseboard");
  }

  private ImmutableComputerSystemOshi(ImmutableComputerSystemOshi.Builder builder) {
    this.manufacturer = builder.manufacturer;
    this.model = builder.model;
    this.firmware = builder.firmware;
    this.baseboard = builder.baseboard;
  }

  /**
   * @return The value of the {@code manufacturer} attribute
   */
  @Override
  public String getManufacturer() {
    return manufacturer;
  }

  /**
   * @return The value of the {@code model} attribute
   */
  @Override
  public String getModel() {
    return model;
  }

  /**
   * @return The value of the {@code firmware} attribute
   */
  @Override
  public FirmwareOshi getFirmware() {
    return firmware;
  }

  /**
   * @return The value of the {@code baseboard} attribute
   */
  @Override
  public BaseboardOshi getBaseboard() {
    return baseboard;
  }

  /**
   * This instance is equal to all instances of {@code ImmutableComputerSystemOshi} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableComputerSystemOshi
        && equalTo((ImmutableComputerSystemOshi) another);
  }

  private boolean equalTo(ImmutableComputerSystemOshi another) {
    return manufacturer.equals(another.manufacturer)
        && model.equals(another.model)
        && firmware.equals(another.firmware)
        && baseboard.equals(another.baseboard);
  }

  /**
   * Computes a hash code from attributes: {@code manufacturer}, {@code model}, {@code firmware}, {@code baseboard}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + manufacturer.hashCode();
    h += (h << 5) + model.hashCode();
    h += (h << 5) + firmware.hashCode();
    h += (h << 5) + baseboard.hashCode();
    return h;
  }


  /**
   * Prints the immutable value {@code ComputerSystemOshi} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ComputerSystemOshi{"
        + "manufacturer=" + manufacturer
        + ", model=" + model
        + ", firmware=" + firmware
        + ", baseboard=" + baseboard
        + "}";
  }

  /**
   * Construct a new immutable {@code ComputerSystemOshi} instance.
   * @param manufacturer The value for the {@code manufacturer} attribute
   * @param model The value for the {@code model} attribute
   * @param firmware The value for the {@code firmware} attribute
   * @param baseboard The value for the {@code baseboard} attribute
   * @return An immutable ComputerSystemOshi instance
   */
  public static ImmutableComputerSystemOshi of(String manufacturer, String model, FirmwareOshi firmware, BaseboardOshi baseboard) {
    return new ImmutableComputerSystemOshi(manufacturer, model, firmware, baseboard);
  }

  /**
   * Creates a builder for {@link ImmutableComputerSystemOshi ImmutableComputerSystemOshi}.
   * <pre>
   * ImmutableComputerSystemOshi.builder()
   *    .manufacturer(String) // required {@link ComputerSystemOshi#getManufacturer() manufacturer}
   *    .model(String) // required {@link ComputerSystemOshi#getModel() model}
   *    .firmware(io.deephaven.process.FirmwareOshi) // required {@link ComputerSystemOshi#getFirmware() firmware}
   *    .baseboard(io.deephaven.process.BaseboardOshi) // required {@link ComputerSystemOshi#getBaseboard() baseboard}
   *    .build();
   * </pre>
   * @return A new ImmutableComputerSystemOshi builder
   */
  public static ImmutableComputerSystemOshi.Builder builder() {
    return new ImmutableComputerSystemOshi.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableComputerSystemOshi ImmutableComputerSystemOshi}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ComputerSystemOshi", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_MANUFACTURER = 0x1L;
    private static final long INIT_BIT_MODEL = 0x2L;
    private static final long INIT_BIT_FIRMWARE = 0x4L;
    private static final long INIT_BIT_BASEBOARD = 0x8L;
    private long initBits = 0xfL;

    private String manufacturer;
    private String model;
    private FirmwareOshi firmware;
    private BaseboardOshi baseboard;

    private Builder() {
    }

    /**
     * Initializes the value for the {@link ComputerSystemOshi#getManufacturer() manufacturer} attribute.
     * @param manufacturer The value for manufacturer 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder manufacturer(String manufacturer) {
      checkNotIsSet(manufacturerIsSet(), "manufacturer");
      this.manufacturer = Objects.requireNonNull(manufacturer, "manufacturer");
      initBits &= ~INIT_BIT_MANUFACTURER;
      return this;
    }

    /**
     * Initializes the value for the {@link ComputerSystemOshi#getModel() model} attribute.
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
     * Initializes the value for the {@link ComputerSystemOshi#getFirmware() firmware} attribute.
     * @param firmware The value for firmware 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder firmware(FirmwareOshi firmware) {
      checkNotIsSet(firmwareIsSet(), "firmware");
      this.firmware = Objects.requireNonNull(firmware, "firmware");
      initBits &= ~INIT_BIT_FIRMWARE;
      return this;
    }

    /**
     * Initializes the value for the {@link ComputerSystemOshi#getBaseboard() baseboard} attribute.
     * @param baseboard The value for baseboard 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder baseboard(BaseboardOshi baseboard) {
      checkNotIsSet(baseboardIsSet(), "baseboard");
      this.baseboard = Objects.requireNonNull(baseboard, "baseboard");
      initBits &= ~INIT_BIT_BASEBOARD;
      return this;
    }

    /**
     * Builds a new {@link ImmutableComputerSystemOshi ImmutableComputerSystemOshi}.
     * @return An immutable instance of ComputerSystemOshi
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableComputerSystemOshi build() {
      checkRequiredAttributes();
      return new ImmutableComputerSystemOshi(this);
    }

    private boolean manufacturerIsSet() {
      return (initBits & INIT_BIT_MANUFACTURER) == 0;
    }

    private boolean modelIsSet() {
      return (initBits & INIT_BIT_MODEL) == 0;
    }

    private boolean firmwareIsSet() {
      return (initBits & INIT_BIT_FIRMWARE) == 0;
    }

    private boolean baseboardIsSet() {
      return (initBits & INIT_BIT_BASEBOARD) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of ComputerSystemOshi is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!manufacturerIsSet()) attributes.add("manufacturer");
      if (!modelIsSet()) attributes.add("model");
      if (!firmwareIsSet()) attributes.add("firmware");
      if (!baseboardIsSet()) attributes.add("baseboard");
      return "Cannot build ComputerSystemOshi, some of required attributes are not set " + attributes;
    }
  }
}
