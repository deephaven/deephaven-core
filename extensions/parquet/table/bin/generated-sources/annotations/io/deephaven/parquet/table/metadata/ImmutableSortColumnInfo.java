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
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link SortColumnInfo}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableSortColumnInfo.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableSortColumnInfo.of()}.
 */
@Generated(from = "SortColumnInfo", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableSortColumnInfo extends SortColumnInfo {
  private final String columnName;
  private final SortColumnInfo.SortDirection sortDirection;

  private ImmutableSortColumnInfo(
      String columnName,
      SortColumnInfo.SortDirection sortDirection) {
    this.columnName = Objects.requireNonNull(columnName, "columnName");
    this.sortDirection = Objects.requireNonNull(sortDirection, "sortDirection");
  }

  private ImmutableSortColumnInfo(
      ImmutableSortColumnInfo original,
      String columnName,
      SortColumnInfo.SortDirection sortDirection) {
    this.columnName = columnName;
    this.sortDirection = sortDirection;
  }

  /**
   * @return The value of the {@code columnName} attribute
   */
  @JsonProperty("columnName")
  @Override
  public String columnName() {
    return columnName;
  }

  /**
   * @return The value of the {@code sortDirection} attribute
   */
  @JsonProperty("sortDirection")
  @Override
  public SortColumnInfo.SortDirection sortDirection() {
    return sortDirection;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SortColumnInfo#columnName() columnName} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for columnName
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSortColumnInfo withColumnName(String value) {
    String newValue = Objects.requireNonNull(value, "columnName");
    if (this.columnName.equals(newValue)) return this;
    return validate(new ImmutableSortColumnInfo(this, newValue, this.sortDirection));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SortColumnInfo#sortDirection() sortDirection} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for sortDirection
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSortColumnInfo withSortDirection(SortColumnInfo.SortDirection value) {
    SortColumnInfo.SortDirection newValue = Objects.requireNonNull(value, "sortDirection");
    if (this.sortDirection == newValue) return this;
    return validate(new ImmutableSortColumnInfo(this, this.columnName, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableSortColumnInfo} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableSortColumnInfo
        && equalTo(0, (ImmutableSortColumnInfo) another);
  }

  private boolean equalTo(int synthetic, ImmutableSortColumnInfo another) {
    return columnName.equals(another.columnName)
        && sortDirection.equals(another.sortDirection);
  }

  /**
   * Computes a hash code from attributes: {@code columnName}, {@code sortDirection}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + columnName.hashCode();
    h += (h << 5) + sortDirection.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code SortColumnInfo} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("SortColumnInfo")
        .omitNullValues()
        .add("columnName", columnName)
        .add("sortDirection", sortDirection)
        .toString();
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "SortColumnInfo", generator = "Immutables")
  @Deprecated
  @SuppressWarnings("Immutable")
  @JsonDeserialize
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends SortColumnInfo {
    @Nullable String columnName;
    @Nullable SortColumnInfo.SortDirection sortDirection;
    @JsonProperty("columnName")
    public void setColumnName(String columnName) {
      this.columnName = columnName;
    }
    @JsonProperty("sortDirection")
    public void setSortDirection(SortColumnInfo.SortDirection sortDirection) {
      this.sortDirection = sortDirection;
    }
    @Override
    public String columnName() { throw new UnsupportedOperationException(); }
    @Override
    public SortColumnInfo.SortDirection sortDirection() { throw new UnsupportedOperationException(); }
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static ImmutableSortColumnInfo fromJson(Json json) {
    ImmutableSortColumnInfo.Builder builder = ImmutableSortColumnInfo.builder();
    if (json.columnName != null) {
      builder.columnName(json.columnName);
    }
    if (json.sortDirection != null) {
      builder.sortDirection(json.sortDirection);
    }
    return builder.build();
  }

  /**
   * Construct a new immutable {@code SortColumnInfo} instance.
   * @param columnName The value for the {@code columnName} attribute
   * @param sortDirection The value for the {@code sortDirection} attribute
   * @return An immutable SortColumnInfo instance
   */
  public static ImmutableSortColumnInfo of(String columnName, SortColumnInfo.SortDirection sortDirection) {
    return validate(new ImmutableSortColumnInfo(columnName, sortDirection));
  }

  private static ImmutableSortColumnInfo validate(ImmutableSortColumnInfo instance) {
    instance.checkColumnName();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link SortColumnInfo} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable SortColumnInfo instance
   */
  public static ImmutableSortColumnInfo copyOf(SortColumnInfo instance) {
    if (instance instanceof ImmutableSortColumnInfo) {
      return (ImmutableSortColumnInfo) instance;
    }
    return ImmutableSortColumnInfo.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableSortColumnInfo ImmutableSortColumnInfo}.
   * <pre>
   * ImmutableSortColumnInfo.builder()
   *    .columnName(String) // required {@link SortColumnInfo#columnName() columnName}
   *    .sortDirection(io.deephaven.parquet.table.metadata.SortColumnInfo.SortDirection) // required {@link SortColumnInfo#sortDirection() sortDirection}
   *    .build();
   * </pre>
   * @return A new ImmutableSortColumnInfo builder
   */
  public static ImmutableSortColumnInfo.Builder builder() {
    return new ImmutableSortColumnInfo.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableSortColumnInfo ImmutableSortColumnInfo}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "SortColumnInfo", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_COLUMN_NAME = 0x1L;
    private static final long INIT_BIT_SORT_DIRECTION = 0x2L;
    private long initBits = 0x3L;

    private @Nullable String columnName;
    private @Nullable SortColumnInfo.SortDirection sortDirection;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code SortColumnInfo} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(SortColumnInfo instance) {
      Objects.requireNonNull(instance, "instance");
      columnName(instance.columnName());
      sortDirection(instance.sortDirection());
      return this;
    }

    /**
     * Initializes the value for the {@link SortColumnInfo#columnName() columnName} attribute.
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
     * Initializes the value for the {@link SortColumnInfo#sortDirection() sortDirection} attribute.
     * @param sortDirection The value for sortDirection 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("sortDirection")
    public final Builder sortDirection(SortColumnInfo.SortDirection sortDirection) {
      this.sortDirection = Objects.requireNonNull(sortDirection, "sortDirection");
      initBits &= ~INIT_BIT_SORT_DIRECTION;
      return this;
    }

    /**
     * Builds a new {@link ImmutableSortColumnInfo ImmutableSortColumnInfo}.
     * @return An immutable instance of SortColumnInfo
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableSortColumnInfo build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableSortColumnInfo.validate(new ImmutableSortColumnInfo(null, columnName, sortDirection));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_COLUMN_NAME) != 0) attributes.add("columnName");
      if ((initBits & INIT_BIT_SORT_DIRECTION) != 0) attributes.add("sortDirection");
      return "Cannot build SortColumnInfo, some of required attributes are not set " + attributes;
    }
  }
}
