package io.deephaven.process;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link OperatingSystemOshi}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableOperatingSystemOshi.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableOperatingSystemOshi.of()}.
 */
@Generated(from = "OperatingSystemOshi", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableOperatingSystemOshi extends OperatingSystemOshi {
  private final String family;
  private final String manufacturer;
  private final OperatingSystemVersionOshi version;
  private final NetworkOshi network;
  private final int pid;

  private ImmutableOperatingSystemOshi(
      String family,
      String manufacturer,
      OperatingSystemVersionOshi version,
      NetworkOshi network,
      int pid) {
    this.family = Objects.requireNonNull(family, "family");
    this.manufacturer = Objects.requireNonNull(manufacturer, "manufacturer");
    this.version = Objects.requireNonNull(version, "version");
    this.network = Objects.requireNonNull(network, "network");
    this.pid = pid;
  }

  private ImmutableOperatingSystemOshi(ImmutableOperatingSystemOshi.Builder builder) {
    this.family = builder.family;
    this.manufacturer = builder.manufacturer;
    this.version = builder.version;
    this.network = builder.network;
    this.pid = builder.pid;
  }

  /**
   * @return The value of the {@code family} attribute
   */
  @Override
  public String getFamily() {
    return family;
  }

  /**
   * @return The value of the {@code manufacturer} attribute
   */
  @Override
  public String getManufacturer() {
    return manufacturer;
  }

  /**
   * @return The value of the {@code version} attribute
   */
  @Override
  public OperatingSystemVersionOshi getVersion() {
    return version;
  }

  /**
   * @return The value of the {@code network} attribute
   */
  @Override
  public NetworkOshi getNetwork() {
    return network;
  }

  /**
   * @return The value of the {@code pid} attribute
   */
  @Override
  public int getPid() {
    return pid;
  }

  /**
   * This instance is equal to all instances of {@code ImmutableOperatingSystemOshi} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableOperatingSystemOshi
        && equalTo((ImmutableOperatingSystemOshi) another);
  }

  private boolean equalTo(ImmutableOperatingSystemOshi another) {
    return family.equals(another.family)
        && manufacturer.equals(another.manufacturer)
        && version.equals(another.version)
        && network.equals(another.network)
        && pid == another.pid;
  }

  /**
   * Computes a hash code from attributes: {@code family}, {@code manufacturer}, {@code version}, {@code network}, {@code pid}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + family.hashCode();
    h += (h << 5) + manufacturer.hashCode();
    h += (h << 5) + version.hashCode();
    h += (h << 5) + network.hashCode();
    h += (h << 5) + pid;
    return h;
  }


  /**
   * Prints the immutable value {@code OperatingSystemOshi} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "OperatingSystemOshi{"
        + "family=" + family
        + ", manufacturer=" + manufacturer
        + ", version=" + version
        + ", network=" + network
        + ", pid=" + pid
        + "}";
  }

  /**
   * Construct a new immutable {@code OperatingSystemOshi} instance.
   * @param family The value for the {@code family} attribute
   * @param manufacturer The value for the {@code manufacturer} attribute
   * @param version The value for the {@code version} attribute
   * @param network The value for the {@code network} attribute
   * @param pid The value for the {@code pid} attribute
   * @return An immutable OperatingSystemOshi instance
   */
  public static ImmutableOperatingSystemOshi of(String family, String manufacturer, OperatingSystemVersionOshi version, NetworkOshi network, int pid) {
    return new ImmutableOperatingSystemOshi(family, manufacturer, version, network, pid);
  }

  /**
   * Creates a builder for {@link ImmutableOperatingSystemOshi ImmutableOperatingSystemOshi}.
   * <pre>
   * ImmutableOperatingSystemOshi.builder()
   *    .family(String) // required {@link OperatingSystemOshi#getFamily() family}
   *    .manufacturer(String) // required {@link OperatingSystemOshi#getManufacturer() manufacturer}
   *    .version(io.deephaven.process.OperatingSystemVersionOshi) // required {@link OperatingSystemOshi#getVersion() version}
   *    .network(io.deephaven.process.NetworkOshi) // required {@link OperatingSystemOshi#getNetwork() network}
   *    .pid(int) // required {@link OperatingSystemOshi#getPid() pid}
   *    .build();
   * </pre>
   * @return A new ImmutableOperatingSystemOshi builder
   */
  public static ImmutableOperatingSystemOshi.Builder builder() {
    return new ImmutableOperatingSystemOshi.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableOperatingSystemOshi ImmutableOperatingSystemOshi}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "OperatingSystemOshi", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_FAMILY = 0x1L;
    private static final long INIT_BIT_MANUFACTURER = 0x2L;
    private static final long INIT_BIT_VERSION = 0x4L;
    private static final long INIT_BIT_NETWORK = 0x8L;
    private static final long INIT_BIT_PID = 0x10L;
    private long initBits = 0x1fL;

    private String family;
    private String manufacturer;
    private OperatingSystemVersionOshi version;
    private NetworkOshi network;
    private int pid;

    private Builder() {
    }

    /**
     * Initializes the value for the {@link OperatingSystemOshi#getFamily() family} attribute.
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
     * Initializes the value for the {@link OperatingSystemOshi#getManufacturer() manufacturer} attribute.
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
     * Initializes the value for the {@link OperatingSystemOshi#getVersion() version} attribute.
     * @param version The value for version 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder version(OperatingSystemVersionOshi version) {
      checkNotIsSet(versionIsSet(), "version");
      this.version = Objects.requireNonNull(version, "version");
      initBits &= ~INIT_BIT_VERSION;
      return this;
    }

    /**
     * Initializes the value for the {@link OperatingSystemOshi#getNetwork() network} attribute.
     * @param network The value for network 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder network(NetworkOshi network) {
      checkNotIsSet(networkIsSet(), "network");
      this.network = Objects.requireNonNull(network, "network");
      initBits &= ~INIT_BIT_NETWORK;
      return this;
    }

    /**
     * Initializes the value for the {@link OperatingSystemOshi#getPid() pid} attribute.
     * @param pid The value for pid 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder pid(int pid) {
      checkNotIsSet(pidIsSet(), "pid");
      this.pid = pid;
      initBits &= ~INIT_BIT_PID;
      return this;
    }

    /**
     * Builds a new {@link ImmutableOperatingSystemOshi ImmutableOperatingSystemOshi}.
     * @return An immutable instance of OperatingSystemOshi
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableOperatingSystemOshi build() {
      checkRequiredAttributes();
      return new ImmutableOperatingSystemOshi(this);
    }

    private boolean familyIsSet() {
      return (initBits & INIT_BIT_FAMILY) == 0;
    }

    private boolean manufacturerIsSet() {
      return (initBits & INIT_BIT_MANUFACTURER) == 0;
    }

    private boolean versionIsSet() {
      return (initBits & INIT_BIT_VERSION) == 0;
    }

    private boolean networkIsSet() {
      return (initBits & INIT_BIT_NETWORK) == 0;
    }

    private boolean pidIsSet() {
      return (initBits & INIT_BIT_PID) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of OperatingSystemOshi is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!familyIsSet()) attributes.add("family");
      if (!manufacturerIsSet()) attributes.add("manufacturer");
      if (!versionIsSet()) attributes.add("version");
      if (!networkIsSet()) attributes.add("network");
      if (!pidIsSet()) attributes.add("pid");
      return "Cannot build OperatingSystemOshi, some of required attributes are not set " + attributes;
    }
  }
}
