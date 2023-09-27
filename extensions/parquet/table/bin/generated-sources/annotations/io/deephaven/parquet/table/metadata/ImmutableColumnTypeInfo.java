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
 * Immutable implementation of {@link ColumnTypeInfo}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableColumnTypeInfo.builder()}.
 */
@Generated(from = "ColumnTypeInfo", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableColumnTypeInfo extends ColumnTypeInfo {
  private final String columnName;
  private final @Nullable CodecInfo codec;
  private final @Nullable ColumnTypeInfo.SpecialType specialType;

  private ImmutableColumnTypeInfo(
      String columnName,
      @Nullable CodecInfo codec,
      @Nullable ColumnTypeInfo.SpecialType specialType) {
    this.columnName = columnName;
    this.codec = codec;
    this.specialType = specialType;
  }

  /**
   * @return The column name
   */
  @JsonProperty("columnName")
  @Override
  public String columnName() {
    return columnName;
  }

  /**
   * @return Optional codec info for binary types
   */
  @JsonProperty("codec")
  @Override
  public Optional<CodecInfo> codec() {
    return Optional.ofNullable(codec);
  }

  /**
   * @return Optional type hint for complex types
   */
  @JsonProperty("specialType")
  @Override
  public Optional<ColumnTypeInfo.SpecialType> specialType() {
    return Optional.ofNullable(specialType);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ColumnTypeInfo#columnName() columnName} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for columnName
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumnTypeInfo withColumnName(String value) {
    String newValue = Objects.requireNonNull(value, "columnName");
    if (this.columnName.equals(newValue)) return this;
    return validate(new ImmutableColumnTypeInfo(newValue, this.codec, this.specialType));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link ColumnTypeInfo#codec() codec} attribute.
   * @param value The value for codec
   * @return A modified copy of {@code this} object
   */
  public final ImmutableColumnTypeInfo withCodec(CodecInfo value) {
    @Nullable CodecInfo newValue = Objects.requireNonNull(value, "codec");
    if (this.codec == newValue) return this;
    return validate(new ImmutableColumnTypeInfo(this.columnName, newValue, this.specialType));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link ColumnTypeInfo#codec() codec} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for codec
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableColumnTypeInfo withCodec(Optional<? extends CodecInfo> optional) {
    @Nullable CodecInfo value = optional.orElse(null);
    if (this.codec == value) return this;
    return validate(new ImmutableColumnTypeInfo(this.columnName, value, this.specialType));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link ColumnTypeInfo#specialType() specialType} attribute.
   * @param value The value for specialType
   * @return A modified copy of {@code this} object
   */
  public final ImmutableColumnTypeInfo withSpecialType(ColumnTypeInfo.SpecialType value) {
    @Nullable ColumnTypeInfo.SpecialType newValue = Objects.requireNonNull(value, "specialType");
    if (this.specialType == newValue) return this;
    return validate(new ImmutableColumnTypeInfo(this.columnName, this.codec, newValue));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link ColumnTypeInfo#specialType() specialType} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for specialType
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableColumnTypeInfo withSpecialType(Optional<? extends ColumnTypeInfo.SpecialType> optional) {
    @Nullable ColumnTypeInfo.SpecialType value = optional.orElse(null);
    if (this.specialType == value) return this;
    return validate(new ImmutableColumnTypeInfo(this.columnName, this.codec, value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableColumnTypeInfo} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableColumnTypeInfo
        && equalTo(0, (ImmutableColumnTypeInfo) another);
  }

  private boolean equalTo(int synthetic, ImmutableColumnTypeInfo another) {
    return columnName.equals(another.columnName)
        && Objects.equals(codec, another.codec)
        && Objects.equals(specialType, another.specialType);
  }

  /**
   * Computes a hash code from attributes: {@code columnName}, {@code codec}, {@code specialType}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + columnName.hashCode();
    h += (h << 5) + Objects.hashCode(codec);
    h += (h << 5) + Objects.hashCode(specialType);
    return h;
  }

  /**
   * Prints the immutable value {@code ColumnTypeInfo} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("ColumnTypeInfo")
        .omitNullValues()
        .add("columnName", columnName)
        .add("codec", codec)
        .add("specialType", specialType)
        .toString();
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "ColumnTypeInfo", generator = "Immutables")
  @Deprecated
  @SuppressWarnings("Immutable")
  @JsonDeserialize
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends ColumnTypeInfo {
    @Nullable String columnName;
    @Nullable Optional<CodecInfo> codec = Optional.empty();
    @Nullable Optional<ColumnTypeInfo.SpecialType> specialType = Optional.empty();
    @JsonProperty("columnName")
    public void setColumnName(String columnName) {
      this.columnName = columnName;
    }
    @JsonProperty("codec")
    public void setCodec(Optional<CodecInfo> codec) {
      this.codec = codec;
    }
    @JsonProperty("specialType")
    public void setSpecialType(Optional<ColumnTypeInfo.SpecialType> specialType) {
      this.specialType = specialType;
    }
    @Override
    public String columnName() { throw new UnsupportedOperationException(); }
    @Override
    public Optional<CodecInfo> codec() { throw new UnsupportedOperationException(); }
    @Override
    public Optional<ColumnTypeInfo.SpecialType> specialType() { throw new UnsupportedOperationException(); }
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static ImmutableColumnTypeInfo fromJson(Json json) {
    ImmutableColumnTypeInfo.Builder builder = ImmutableColumnTypeInfo.builder();
    if (json.columnName != null) {
      builder.columnName(json.columnName);
    }
    if (json.codec != null) {
      builder.codec(json.codec);
    }
    if (json.specialType != null) {
      builder.specialType(json.specialType);
    }
    return builder.build();
  }

  private static ImmutableColumnTypeInfo validate(ImmutableColumnTypeInfo instance) {
    instance.checkColumnName();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link ColumnTypeInfo} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ColumnTypeInfo instance
   */
  public static ImmutableColumnTypeInfo copyOf(ColumnTypeInfo instance) {
    if (instance instanceof ImmutableColumnTypeInfo) {
      return (ImmutableColumnTypeInfo) instance;
    }
    return ImmutableColumnTypeInfo.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableColumnTypeInfo ImmutableColumnTypeInfo}.
   * <pre>
   * ImmutableColumnTypeInfo.builder()
   *    .columnName(String) // required {@link ColumnTypeInfo#columnName() columnName}
   *    .codec(io.deephaven.parquet.table.metadata.CodecInfo) // optional {@link ColumnTypeInfo#codec() codec}
   *    .specialType(io.deephaven.parquet.table.metadata.ColumnTypeInfo.SpecialType) // optional {@link ColumnTypeInfo#specialType() specialType}
   *    .build();
   * </pre>
   * @return A new ImmutableColumnTypeInfo builder
   */
  public static ImmutableColumnTypeInfo.Builder builder() {
    return new ImmutableColumnTypeInfo.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableColumnTypeInfo ImmutableColumnTypeInfo}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ColumnTypeInfo", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements ColumnTypeInfo.Builder {
    private static final long INIT_BIT_COLUMN_NAME = 0x1L;
    private long initBits = 0x1L;

    private @Nullable String columnName;
    private @Nullable CodecInfo codec;
    private @Nullable ColumnTypeInfo.SpecialType specialType;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ColumnTypeInfo} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(ColumnTypeInfo instance) {
      Objects.requireNonNull(instance, "instance");
      columnName(instance.columnName());
      Optional<CodecInfo> codecOptional = instance.codec();
      if (codecOptional.isPresent()) {
        codec(codecOptional);
      }
      Optional<ColumnTypeInfo.SpecialType> specialTypeOptional = instance.specialType();
      if (specialTypeOptional.isPresent()) {
        specialType(specialTypeOptional);
      }
      return this;
    }

    /**
     * Initializes the value for the {@link ColumnTypeInfo#columnName() columnName} attribute.
     * @param columnName The value for columnName 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("columnName")
    public final Builder columnName(String columnName) {
      this.columnName = Objects.requireNonNull(columnName, "columnName");
      initBits &= ~INIT_BIT_COLUMN_NAME;
      return this;
    }

    /**
     * Initializes the optional value {@link ColumnTypeInfo#codec() codec} to codec.
     * @param codec The value for codec
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder codec(CodecInfo codec) {
      this.codec = Objects.requireNonNull(codec, "codec");
      return this;
    }

    /**
     * Initializes the optional value {@link ColumnTypeInfo#codec() codec} to codec.
     * @param codec The value for codec
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("codec")
    public final Builder codec(Optional<? extends CodecInfo> codec) {
      this.codec = codec.orElse(null);
      return this;
    }

    /**
     * Initializes the optional value {@link ColumnTypeInfo#specialType() specialType} to specialType.
     * @param specialType The value for specialType
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder specialType(ColumnTypeInfo.SpecialType specialType) {
      this.specialType = Objects.requireNonNull(specialType, "specialType");
      return this;
    }

    /**
     * Initializes the optional value {@link ColumnTypeInfo#specialType() specialType} to specialType.
     * @param specialType The value for specialType
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("specialType")
    public final Builder specialType(Optional<? extends ColumnTypeInfo.SpecialType> specialType) {
      this.specialType = specialType.orElse(null);
      return this;
    }

    /**
     * Builds a new {@link ImmutableColumnTypeInfo ImmutableColumnTypeInfo}.
     * @return An immutable instance of ColumnTypeInfo
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableColumnTypeInfo build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableColumnTypeInfo.validate(new ImmutableColumnTypeInfo(columnName, codec, specialType));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_COLUMN_NAME) != 0) attributes.add("columnName");
      return "Cannot build ColumnTypeInfo, some of required attributes are not set " + attributes;
    }
  }
}
