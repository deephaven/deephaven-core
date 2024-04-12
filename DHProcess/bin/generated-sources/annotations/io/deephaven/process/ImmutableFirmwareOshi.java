package io.deephaven.process;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link FirmwareOshi}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableFirmwareOshi.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableFirmwareOshi.of()}.
 */
@Generated(from = "FirmwareOshi", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableFirmwareOshi extends FirmwareOshi {
  private final String manufacturer;
  private final String name;
  private final String description;
  private final String version;
  private final String releaseDate;

  private ImmutableFirmwareOshi(
      String manufacturer,
      String name,
      String description,
      String version,
      String releaseDate) {
    this.manufacturer = Objects.requireNonNull(manufacturer, "manufacturer");
    this.name = Objects.requireNonNull(name, "name");
    this.description = Objects.requireNonNull(description, "description");
    this.version = Objects.requireNonNull(version, "version");
    this.releaseDate = Objects.requireNonNull(releaseDate, "releaseDate");
  }

  private ImmutableFirmwareOshi(ImmutableFirmwareOshi.Builder builder) {
    this.manufacturer = builder.manufacturer;
    this.name = builder.name;
    this.description = builder.description;
    this.version = builder.version;
    this.releaseDate = builder.releaseDate;
  }

  /**
   * @return The value of the {@code manufacturer} attribute
   */
  @Override
  public String getManufacturer() {
    return manufacturer;
  }

  /**
   * @return The value of the {@code name} attribute
   */
  @Override
  public String getName() {
    return name;
  }

  /**
   * @return The value of the {@code description} attribute
   */
  @Override
  public String getDescription() {
    return description;
  }

  /**
   * @return The value of the {@code version} attribute
   */
  @Override
  public String getVersion() {
    return version;
  }

  /**
   * @return The value of the {@code releaseDate} attribute
   */
  @Override
  public String getReleaseDate() {
    return releaseDate;
  }

  /**
   * This instance is equal to all instances of {@code ImmutableFirmwareOshi} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableFirmwareOshi
        && equalTo((ImmutableFirmwareOshi) another);
  }

  private boolean equalTo(ImmutableFirmwareOshi another) {
    return manufacturer.equals(another.manufacturer)
        && name.equals(another.name)
        && description.equals(another.description)
        && version.equals(another.version)
        && releaseDate.equals(another.releaseDate);
  }

  /**
   * Computes a hash code from attributes: {@code manufacturer}, {@code name}, {@code description}, {@code version}, {@code releaseDate}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + manufacturer.hashCode();
    h += (h << 5) + name.hashCode();
    h += (h << 5) + description.hashCode();
    h += (h << 5) + version.hashCode();
    h += (h << 5) + releaseDate.hashCode();
    return h;
  }


  /**
   * Prints the immutable value {@code FirmwareOshi} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "FirmwareOshi{"
        + "manufacturer=" + manufacturer
        + ", name=" + name
        + ", description=" + description
        + ", version=" + version
        + ", releaseDate=" + releaseDate
        + "}";
  }

  /**
   * Construct a new immutable {@code FirmwareOshi} instance.
   * @param manufacturer The value for the {@code manufacturer} attribute
   * @param name The value for the {@code name} attribute
   * @param description The value for the {@code description} attribute
   * @param version The value for the {@code version} attribute
   * @param releaseDate The value for the {@code releaseDate} attribute
   * @return An immutable FirmwareOshi instance
   */
  public static ImmutableFirmwareOshi of(String manufacturer, String name, String description, String version, String releaseDate) {
    return new ImmutableFirmwareOshi(manufacturer, name, description, version, releaseDate);
  }

  /**
   * Creates a builder for {@link ImmutableFirmwareOshi ImmutableFirmwareOshi}.
   * <pre>
   * ImmutableFirmwareOshi.builder()
   *    .manufacturer(String) // required {@link FirmwareOshi#getManufacturer() manufacturer}
   *    .name(String) // required {@link FirmwareOshi#getName() name}
   *    .description(String) // required {@link FirmwareOshi#getDescription() description}
   *    .version(String) // required {@link FirmwareOshi#getVersion() version}
   *    .releaseDate(String) // required {@link FirmwareOshi#getReleaseDate() releaseDate}
   *    .build();
   * </pre>
   * @return A new ImmutableFirmwareOshi builder
   */
  public static ImmutableFirmwareOshi.Builder builder() {
    return new ImmutableFirmwareOshi.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableFirmwareOshi ImmutableFirmwareOshi}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "FirmwareOshi", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_MANUFACTURER = 0x1L;
    private static final long INIT_BIT_NAME = 0x2L;
    private static final long INIT_BIT_DESCRIPTION = 0x4L;
    private static final long INIT_BIT_VERSION = 0x8L;
    private static final long INIT_BIT_RELEASE_DATE = 0x10L;
    private long initBits = 0x1fL;

    private String manufacturer;
    private String name;
    private String description;
    private String version;
    private String releaseDate;

    private Builder() {
    }

    /**
     * Initializes the value for the {@link FirmwareOshi#getManufacturer() manufacturer} attribute.
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
     * Initializes the value for the {@link FirmwareOshi#getName() name} attribute.
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
     * Initializes the value for the {@link FirmwareOshi#getDescription() description} attribute.
     * @param description The value for description 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder description(String description) {
      checkNotIsSet(descriptionIsSet(), "description");
      this.description = Objects.requireNonNull(description, "description");
      initBits &= ~INIT_BIT_DESCRIPTION;
      return this;
    }

    /**
     * Initializes the value for the {@link FirmwareOshi#getVersion() version} attribute.
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
     * Initializes the value for the {@link FirmwareOshi#getReleaseDate() releaseDate} attribute.
     * @param releaseDate The value for releaseDate 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder releaseDate(String releaseDate) {
      checkNotIsSet(releaseDateIsSet(), "releaseDate");
      this.releaseDate = Objects.requireNonNull(releaseDate, "releaseDate");
      initBits &= ~INIT_BIT_RELEASE_DATE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableFirmwareOshi ImmutableFirmwareOshi}.
     * @return An immutable instance of FirmwareOshi
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableFirmwareOshi build() {
      checkRequiredAttributes();
      return new ImmutableFirmwareOshi(this);
    }

    private boolean manufacturerIsSet() {
      return (initBits & INIT_BIT_MANUFACTURER) == 0;
    }

    private boolean nameIsSet() {
      return (initBits & INIT_BIT_NAME) == 0;
    }

    private boolean descriptionIsSet() {
      return (initBits & INIT_BIT_DESCRIPTION) == 0;
    }

    private boolean versionIsSet() {
      return (initBits & INIT_BIT_VERSION) == 0;
    }

    private boolean releaseDateIsSet() {
      return (initBits & INIT_BIT_RELEASE_DATE) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of FirmwareOshi is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!manufacturerIsSet()) attributes.add("manufacturer");
      if (!nameIsSet()) attributes.add("name");
      if (!descriptionIsSet()) attributes.add("description");
      if (!versionIsSet()) attributes.add("version");
      if (!releaseDateIsSet()) attributes.add("releaseDate");
      return "Cannot build FirmwareOshi, some of required attributes are not set " + attributes;
    }
  }
}
