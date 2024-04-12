package io.deephaven.process;

import java.util.Objects;
import java.util.Optional;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link OperatingSystemVersionOshi}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableOperatingSystemVersionOshi.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableOperatingSystemVersionOshi.of()}.
 */
@Generated(from = "OperatingSystemVersionOshi", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableOperatingSystemVersionOshi extends OperatingSystemVersionOshi {
  private final String version;
  private final String codeName;
  private final String buildNumber;

  private ImmutableOperatingSystemVersionOshi(
      Optional<String> version,
      Optional<String> codeName,
      Optional<String> buildNumber) {
    this.version = version.orElse(null);
    this.codeName = codeName.orElse(null);
    this.buildNumber = buildNumber.orElse(null);
  }

  private ImmutableOperatingSystemVersionOshi(ImmutableOperatingSystemVersionOshi.Builder builder) {
    this.version = builder.version;
    this.codeName = builder.codeName;
    this.buildNumber = builder.buildNumber;
  }

  /**
   * @return The value of the {@code version} attribute
   */
  @Override
  public Optional<String> getVersion() {
    return Optional.ofNullable(version);
  }

  /**
   * @return The value of the {@code codeName} attribute
   */
  @Override
  public Optional<String> getCodeName() {
    return Optional.ofNullable(codeName);
  }

  /**
   * @return The value of the {@code buildNumber} attribute
   */
  @Override
  public Optional<String> getBuildNumber() {
    return Optional.ofNullable(buildNumber);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableOperatingSystemVersionOshi} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableOperatingSystemVersionOshi
        && equalTo((ImmutableOperatingSystemVersionOshi) another);
  }

  private boolean equalTo(ImmutableOperatingSystemVersionOshi another) {
    return Objects.equals(version, another.version)
        && Objects.equals(codeName, another.codeName)
        && Objects.equals(buildNumber, another.buildNumber);
  }

  /**
   * Computes a hash code from attributes: {@code version}, {@code codeName}, {@code buildNumber}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Objects.hashCode(version);
    h += (h << 5) + Objects.hashCode(codeName);
    h += (h << 5) + Objects.hashCode(buildNumber);
    return h;
  }


  /**
   * Prints the immutable value {@code OperatingSystemVersionOshi} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("OperatingSystemVersionOshi{");
    if (version != null) {
      builder.append("version=").append(version);
    }
    if (codeName != null) {
      if (builder.length() > 27) builder.append(", ");
      builder.append("codeName=").append(codeName);
    }
    if (buildNumber != null) {
      if (builder.length() > 27) builder.append(", ");
      builder.append("buildNumber=").append(buildNumber);
    }
    return builder.append("}").toString();
  }

  /**
   * Construct a new immutable {@code OperatingSystemVersionOshi} instance.
   * @param version The value for the {@code version} attribute
   * @param codeName The value for the {@code codeName} attribute
   * @param buildNumber The value for the {@code buildNumber} attribute
   * @return An immutable OperatingSystemVersionOshi instance
   */
  public static ImmutableOperatingSystemVersionOshi of(Optional<String> version, Optional<String> codeName, Optional<String> buildNumber) {
    return new ImmutableOperatingSystemVersionOshi(version, codeName, buildNumber);
  }

  /**
   * Creates a builder for {@link ImmutableOperatingSystemVersionOshi ImmutableOperatingSystemVersionOshi}.
   * <pre>
   * ImmutableOperatingSystemVersionOshi.builder()
   *    .version(String) // optional {@link OperatingSystemVersionOshi#getVersion() version}
   *    .codeName(String) // optional {@link OperatingSystemVersionOshi#getCodeName() codeName}
   *    .buildNumber(String) // optional {@link OperatingSystemVersionOshi#getBuildNumber() buildNumber}
   *    .build();
   * </pre>
   * @return A new ImmutableOperatingSystemVersionOshi builder
   */
  public static ImmutableOperatingSystemVersionOshi.Builder builder() {
    return new ImmutableOperatingSystemVersionOshi.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableOperatingSystemVersionOshi ImmutableOperatingSystemVersionOshi}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "OperatingSystemVersionOshi", generator = "Immutables")
  public static final class Builder {
    private static final long OPT_BIT_VERSION = 0x1L;
    private static final long OPT_BIT_CODE_NAME = 0x2L;
    private static final long OPT_BIT_BUILD_NUMBER = 0x4L;
    private long optBits;

    private String version;
    private String codeName;
    private String buildNumber;

    private Builder() {
    }

    /**
     * Initializes the optional value {@link OperatingSystemVersionOshi#getVersion() version} to version.
     * @param version The value for version
     * @return {@code this} builder for chained invocation
     */
    public final Builder version(String version) {
      checkNotIsSet(versionIsSet(), "version");
      this.version = Objects.requireNonNull(version, "version");
      optBits |= OPT_BIT_VERSION;
      return this;
    }

    /**
     * Initializes the optional value {@link OperatingSystemVersionOshi#getVersion() version} to version.
     * @param version The value for version
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder version(Optional<String> version) {
      checkNotIsSet(versionIsSet(), "version");
      this.version = version.orElse(null);
      optBits |= OPT_BIT_VERSION;
      return this;
    }

    /**
     * Initializes the optional value {@link OperatingSystemVersionOshi#getCodeName() codeName} to codeName.
     * @param codeName The value for codeName
     * @return {@code this} builder for chained invocation
     */
    public final Builder codeName(String codeName) {
      checkNotIsSet(codeNameIsSet(), "codeName");
      this.codeName = Objects.requireNonNull(codeName, "codeName");
      optBits |= OPT_BIT_CODE_NAME;
      return this;
    }

    /**
     * Initializes the optional value {@link OperatingSystemVersionOshi#getCodeName() codeName} to codeName.
     * @param codeName The value for codeName
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder codeName(Optional<String> codeName) {
      checkNotIsSet(codeNameIsSet(), "codeName");
      this.codeName = codeName.orElse(null);
      optBits |= OPT_BIT_CODE_NAME;
      return this;
    }

    /**
     * Initializes the optional value {@link OperatingSystemVersionOshi#getBuildNumber() buildNumber} to buildNumber.
     * @param buildNumber The value for buildNumber
     * @return {@code this} builder for chained invocation
     */
    public final Builder buildNumber(String buildNumber) {
      checkNotIsSet(buildNumberIsSet(), "buildNumber");
      this.buildNumber = Objects.requireNonNull(buildNumber, "buildNumber");
      optBits |= OPT_BIT_BUILD_NUMBER;
      return this;
    }

    /**
     * Initializes the optional value {@link OperatingSystemVersionOshi#getBuildNumber() buildNumber} to buildNumber.
     * @param buildNumber The value for buildNumber
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder buildNumber(Optional<String> buildNumber) {
      checkNotIsSet(buildNumberIsSet(), "buildNumber");
      this.buildNumber = buildNumber.orElse(null);
      optBits |= OPT_BIT_BUILD_NUMBER;
      return this;
    }

    /**
     * Builds a new {@link ImmutableOperatingSystemVersionOshi ImmutableOperatingSystemVersionOshi}.
     * @return An immutable instance of OperatingSystemVersionOshi
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableOperatingSystemVersionOshi build() {
      return new ImmutableOperatingSystemVersionOshi(this);
    }

    private boolean versionIsSet() {
      return (optBits & OPT_BIT_VERSION) != 0;
    }

    private boolean codeNameIsSet() {
      return (optBits & OPT_BIT_CODE_NAME) != 0;
    }

    private boolean buildNumberIsSet() {
      return (optBits & OPT_BIT_BUILD_NUMBER) != 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of OperatingSystemVersionOshi is strict, attribute is already set: ".concat(name));
    }
  }
}
