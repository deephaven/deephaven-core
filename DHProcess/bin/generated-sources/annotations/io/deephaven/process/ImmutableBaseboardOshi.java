package io.deephaven.process;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link BaseboardOshi}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableBaseboardOshi.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableBaseboardOshi.of()}.
 */
@Generated(from = "BaseboardOshi", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableBaseboardOshi extends BaseboardOshi {
  private final String manufacturer;
  private final String model;
  private final String version;
  private final String serialNumber;

  private ImmutableBaseboardOshi(
      String manufacturer,
      String model,
      String version,
      String serialNumber) {
    this.manufacturer = Objects.requireNonNull(manufacturer, "manufacturer");
    this.model = Objects.requireNonNull(model, "model");
    this.version = Objects.requireNonNull(version, "version");
    this.serialNumber = Objects.requireNonNull(serialNumber, "serialNumber");
  }

  private ImmutableBaseboardOshi(ImmutableBaseboardOshi.Builder builder) {
    this.manufacturer = builder.manufacturer;
    this.model = builder.model;
    this.version = builder.version;
    this.serialNumber = builder.serialNumber;
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
   * @return The value of the {@code version} attribute
   */
  @Override
  public String getVersion() {
    return version;
  }

  /**
   * @return The value of the {@code serialNumber} attribute
   */
  @Override
  public String getSerialNumber() {
    return serialNumber;
  }

  /**
   * This instance is equal to all instances of {@code ImmutableBaseboardOshi} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableBaseboardOshi
        && equalTo((ImmutableBaseboardOshi) another);
  }

  private boolean equalTo(ImmutableBaseboardOshi another) {
    return manufacturer.equals(another.manufacturer)
        && model.equals(another.model)
        && version.equals(another.version)
        && serialNumber.equals(another.serialNumber);
  }

  /**
   * Computes a hash code from attributes: {@code manufacturer}, {@code model}, {@code version}, {@code serialNumber}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + manufacturer.hashCode();
    h += (h << 5) + model.hashCode();
    h += (h << 5) + version.hashCode();
    h += (h << 5) + serialNumber.hashCode();
    return h;
  }


  /**
   * Prints the immutable value {@code BaseboardOshi} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "BaseboardOshi{"
        + "manufacturer=" + manufacturer
        + ", model=" + model
        + ", version=" + version
        + ", serialNumber=" + serialNumber
        + "}";
  }

  /**
   * Construct a new immutable {@code BaseboardOshi} instance.
   * @param manufacturer The value for the {@code manufacturer} attribute
   * @param model The value for the {@code model} attribute
   * @param version The value for the {@code version} attribute
   * @param serialNumber The value for the {@code serialNumber} attribute
   * @return An immutable BaseboardOshi instance
   */
  public static ImmutableBaseboardOshi of(String manufacturer, String model, String version, String serialNumber) {
    return new ImmutableBaseboardOshi(manufacturer, model, version, serialNumber);
  }

  /**
   * Creates a builder for {@link ImmutableBaseboardOshi ImmutableBaseboardOshi}.
   * <pre>
   * ImmutableBaseboardOshi.builder()
   *    .manufacturer(String) // required {@link BaseboardOshi#getManufacturer() manufacturer}
   *    .model(String) // required {@link BaseboardOshi#getModel() model}
   *    .version(String) // required {@link BaseboardOshi#getVersion() version}
   *    .serialNumber(String) // required {@link BaseboardOshi#getSerialNumber() serialNumber}
   *    .build();
   * </pre>
   * @return A new ImmutableBaseboardOshi builder
   */
  public static ImmutableBaseboardOshi.Builder builder() {
    return new ImmutableBaseboardOshi.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableBaseboardOshi ImmutableBaseboardOshi}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "BaseboardOshi", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_MANUFACTURER = 0x1L;
    private static final long INIT_BIT_MODEL = 0x2L;
    private static final long INIT_BIT_VERSION = 0x4L;
    private static final long INIT_BIT_SERIAL_NUMBER = 0x8L;
    private long initBits = 0xfL;

    private String manufacturer;
    private String model;
    private String version;
    private String serialNumber;

    private Builder() {
    }

    /**
     * Initializes the value for the {@link BaseboardOshi#getManufacturer() manufacturer} attribute.
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
     * Initializes the value for the {@link BaseboardOshi#getModel() model} attribute.
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
     * Initializes the value for the {@link BaseboardOshi#getVersion() version} attribute.
     * @param version The value for version 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder version(String version) {
      checkNotIsSet(versionIsSet(), "version");
      this.version = Objects.requireNonNull(version, "version");
      initBits &= ~INIT_BIT_VERSION;
      return this;
    }

    /**
     * Initializes the value for the {@link BaseboardOshi#getSerialNumber() serialNumber} attribute.
     * @param serialNumber The value for serialNumber 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder serialNumber(String serialNumber) {
      checkNotIsSet(serialNumberIsSet(), "serialNumber");
      this.serialNumber = Objects.requireNonNull(serialNumber, "serialNumber");
      initBits &= ~INIT_BIT_SERIAL_NUMBER;
      return this;
    }

    /**
     * Builds a new {@link ImmutableBaseboardOshi ImmutableBaseboardOshi}.
     * @return An immutable instance of BaseboardOshi
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableBaseboardOshi build() {
      checkRequiredAttributes();
      return new ImmutableBaseboardOshi(this);
    }

    private boolean manufacturerIsSet() {
      return (initBits & INIT_BIT_MANUFACTURER) == 0;
    }

    private boolean modelIsSet() {
      return (initBits & INIT_BIT_MODEL) == 0;
    }

    private boolean versionIsSet() {
      return (initBits & INIT_BIT_VERSION) == 0;
    }

    private boolean serialNumberIsSet() {
      return (initBits & INIT_BIT_SERIAL_NUMBER) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of BaseboardOshi is strict, attribute is already set: ".concat(name));
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
      if (!versionIsSet()) attributes.add("version");
      if (!serialNumberIsSet()) attributes.add("serialNumber");
      return "Cannot build BaseboardOshi, some of required attributes are not set " + attributes;
    }
  }
}
