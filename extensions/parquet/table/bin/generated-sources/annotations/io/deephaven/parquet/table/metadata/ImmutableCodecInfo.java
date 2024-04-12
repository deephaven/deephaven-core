package io.deephaven.parquet.table.metadata;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link CodecInfo}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableCodecInfo.builder()}.
 */
@Generated(from = "CodecInfo", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableCodecInfo extends CodecInfo {
  private final String codecName;
  private final @Nullable String codecArg;
  private final String dataType;
  private final @Nullable String componentType;

  private ImmutableCodecInfo(
      String codecName,
      @Nullable String codecArg,
      String dataType,
      @Nullable String componentType) {
    this.codecName = codecName;
    this.codecArg = codecArg;
    this.dataType = dataType;
    this.componentType = componentType;
  }

  /**
   * @return Codec name, currently always a Java class name
   */
  @JsonProperty("codecName")
  @Override
  public String codecName() {
    return codecName;
  }

  /**
   * @return Implementation-defined argument string
   */
  @JsonProperty("codecArg")
  @Override
  public Optional<String> codecArg() {
    return Optional.ofNullable(codecArg);
  }

  /**
   * @return Data type produced by the codec, currently always a Java class name
   */
  @JsonProperty("dataType")
  @Override
  public String dataType() {
    return dataType;
  }

  /**
   * @return Component type for array and vector data types, currently always a Java class name
   */
  @JsonProperty("componentType")
  @Override
  public Optional<String> componentType() {
    return Optional.ofNullable(componentType);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CodecInfo#codecName() codecName} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for codecName
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCodecInfo withCodecName(String value) {
    String newValue = Objects.requireNonNull(value, "codecName");
    if (this.codecName.equals(newValue)) return this;
    return validate(new ImmutableCodecInfo(newValue, this.codecArg, this.dataType, this.componentType));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link CodecInfo#codecArg() codecArg} attribute.
   * @param value The value for codecArg
   * @return A modified copy of {@code this} object
   */
  public final ImmutableCodecInfo withCodecArg(String value) {
    @Nullable String newValue = Objects.requireNonNull(value, "codecArg");
    if (Objects.equals(this.codecArg, newValue)) return this;
    return validate(new ImmutableCodecInfo(this.codecName, newValue, this.dataType, this.componentType));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link CodecInfo#codecArg() codecArg} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for codecArg
   * @return A modified copy of {@code this} object
   */
  public final ImmutableCodecInfo withCodecArg(Optional<String> optional) {
    @Nullable String value = optional.orElse(null);
    if (Objects.equals(this.codecArg, value)) return this;
    return validate(new ImmutableCodecInfo(this.codecName, value, this.dataType, this.componentType));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CodecInfo#dataType() dataType} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for dataType
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCodecInfo withDataType(String value) {
    String newValue = Objects.requireNonNull(value, "dataType");
    if (this.dataType.equals(newValue)) return this;
    return validate(new ImmutableCodecInfo(this.codecName, this.codecArg, newValue, this.componentType));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link CodecInfo#componentType() componentType} attribute.
   * @param value The value for componentType
   * @return A modified copy of {@code this} object
   */
  public final ImmutableCodecInfo withComponentType(String value) {
    @Nullable String newValue = Objects.requireNonNull(value, "componentType");
    if (Objects.equals(this.componentType, newValue)) return this;
    return validate(new ImmutableCodecInfo(this.codecName, this.codecArg, this.dataType, newValue));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link CodecInfo#componentType() componentType} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for componentType
   * @return A modified copy of {@code this} object
   */
  public final ImmutableCodecInfo withComponentType(Optional<String> optional) {
    @Nullable String value = optional.orElse(null);
    if (Objects.equals(this.componentType, value)) return this;
    return validate(new ImmutableCodecInfo(this.codecName, this.codecArg, this.dataType, value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableCodecInfo} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableCodecInfo
        && equalTo(0, (ImmutableCodecInfo) another);
  }

  private boolean equalTo(int synthetic, ImmutableCodecInfo another) {
    return codecName.equals(another.codecName)
        && Objects.equals(codecArg, another.codecArg)
        && dataType.equals(another.dataType)
        && Objects.equals(componentType, another.componentType);
  }

  /**
   * Computes a hash code from attributes: {@code codecName}, {@code codecArg}, {@code dataType}, {@code componentType}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + codecName.hashCode();
    h += (h << 5) + Objects.hashCode(codecArg);
    h += (h << 5) + dataType.hashCode();
    h += (h << 5) + Objects.hashCode(componentType);
    return h;
  }

  /**
   * Prints the immutable value {@code CodecInfo} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("CodecInfo")
        .omitNullValues()
        .add("codecName", codecName)
        .add("codecArg", codecArg)
        .add("dataType", dataType)
        .add("componentType", componentType)
        .toString();
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "CodecInfo", generator = "Immutables")
  @Deprecated
  @SuppressWarnings("Immutable")
  @JsonDeserialize
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends CodecInfo {
    @Nullable String codecName;
    @Nullable Optional<String> codecArg = Optional.empty();
    @Nullable String dataType;
    @Nullable Optional<String> componentType = Optional.empty();
    @JsonProperty("codecName")
    public void setCodecName(String codecName) {
      this.codecName = codecName;
    }
    @JsonProperty("codecArg")
    public void setCodecArg(Optional<String> codecArg) {
      this.codecArg = codecArg;
    }
    @JsonProperty("dataType")
    public void setDataType(String dataType) {
      this.dataType = dataType;
    }
    @JsonProperty("componentType")
    public void setComponentType(Optional<String> componentType) {
      this.componentType = componentType;
    }
    @Override
    public String codecName() { throw new UnsupportedOperationException(); }
    @Override
    public Optional<String> codecArg() { throw new UnsupportedOperationException(); }
    @Override
    public String dataType() { throw new UnsupportedOperationException(); }
    @Override
    public Optional<String> componentType() { throw new UnsupportedOperationException(); }
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static ImmutableCodecInfo fromJson(Json json) {
    ImmutableCodecInfo.Builder builder = ImmutableCodecInfo.builder();
    if (json.codecName != null) {
      builder.codecName(json.codecName);
    }
    if (json.codecArg != null) {
      builder.codecArg(json.codecArg);
    }
    if (json.dataType != null) {
      builder.dataType(json.dataType);
    }
    if (json.componentType != null) {
      builder.componentType(json.componentType);
    }
    return builder.build();
  }

  private static ImmutableCodecInfo validate(ImmutableCodecInfo instance) {
    instance.checkComponentType();
    instance.checkDataType();
    instance.checkCodecName();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link CodecInfo} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable CodecInfo instance
   */
  public static ImmutableCodecInfo copyOf(CodecInfo instance) {
    if (instance instanceof ImmutableCodecInfo) {
      return (ImmutableCodecInfo) instance;
    }
    return ImmutableCodecInfo.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableCodecInfo ImmutableCodecInfo}.
   * <pre>
   * ImmutableCodecInfo.builder()
   *    .codecName(String) // required {@link CodecInfo#codecName() codecName}
   *    .codecArg(String) // optional {@link CodecInfo#codecArg() codecArg}
   *    .dataType(String) // required {@link CodecInfo#dataType() dataType}
   *    .componentType(String) // optional {@link CodecInfo#componentType() componentType}
   *    .build();
   * </pre>
   * @return A new ImmutableCodecInfo builder
   */
  public static ImmutableCodecInfo.Builder builder() {
    return new ImmutableCodecInfo.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableCodecInfo ImmutableCodecInfo}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "CodecInfo", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements CodecInfo.Builder {
    private static final long INIT_BIT_CODEC_NAME = 0x1L;
    private static final long INIT_BIT_DATA_TYPE = 0x2L;
    private long initBits = 0x3L;

    private @Nullable String codecName;
    private @Nullable String codecArg;
    private @Nullable String dataType;
    private @Nullable String componentType;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code CodecInfo} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(CodecInfo instance) {
      Objects.requireNonNull(instance, "instance");
      codecName(instance.codecName());
      Optional<String> codecArgOptional = instance.codecArg();
      if (codecArgOptional.isPresent()) {
        codecArg(codecArgOptional);
      }
      dataType(instance.dataType());
      Optional<String> componentTypeOptional = instance.componentType();
      if (componentTypeOptional.isPresent()) {
        componentType(componentTypeOptional);
      }
      return this;
    }

    /**
     * Initializes the value for the {@link CodecInfo#codecName() codecName} attribute.
     * @param codecName The value for codecName 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("codecName")
    public final Builder codecName(String codecName) {
      this.codecName = Objects.requireNonNull(codecName, "codecName");
      initBits &= ~INIT_BIT_CODEC_NAME;
      return this;
    }

    /**
     * Initializes the optional value {@link CodecInfo#codecArg() codecArg} to codecArg.
     * @param codecArg The value for codecArg
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder codecArg(String codecArg) {
      this.codecArg = Objects.requireNonNull(codecArg, "codecArg");
      return this;
    }

    /**
     * Initializes the optional value {@link CodecInfo#codecArg() codecArg} to codecArg.
     * @param codecArg The value for codecArg
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("codecArg")
    public final Builder codecArg(Optional<String> codecArg) {
      this.codecArg = codecArg.orElse(null);
      return this;
    }

    /**
     * Initializes the value for the {@link CodecInfo#dataType() dataType} attribute.
     * @param dataType The value for dataType 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("dataType")
    public final Builder dataType(String dataType) {
      this.dataType = Objects.requireNonNull(dataType, "dataType");
      initBits &= ~INIT_BIT_DATA_TYPE;
      return this;
    }

    /**
     * Initializes the optional value {@link CodecInfo#componentType() componentType} to componentType.
     * @param componentType The value for componentType
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder componentType(String componentType) {
      this.componentType = Objects.requireNonNull(componentType, "componentType");
      return this;
    }

    /**
     * Initializes the optional value {@link CodecInfo#componentType() componentType} to componentType.
     * @param componentType The value for componentType
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("componentType")
    public final Builder componentType(Optional<String> componentType) {
      this.componentType = componentType.orElse(null);
      return this;
    }

    /**
     * Builds a new {@link ImmutableCodecInfo ImmutableCodecInfo}.
     * @return An immutable instance of CodecInfo
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableCodecInfo build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableCodecInfo.validate(new ImmutableCodecInfo(codecName, codecArg, dataType, componentType));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_CODEC_NAME) != 0) attributes.add("codecName");
      if ((initBits & INIT_BIT_DATA_TYPE) != 0) attributes.add("dataType");
      return "Cannot build CodecInfo, some of required attributes are not set " + attributes;
    }
  }
}
