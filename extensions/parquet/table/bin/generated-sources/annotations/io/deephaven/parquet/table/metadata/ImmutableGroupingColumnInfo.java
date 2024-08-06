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
 * Immutable implementation of {@link GroupingColumnInfo}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableGroupingColumnInfo.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableGroupingColumnInfo.of()}.
 */
@Generated(from = "GroupingColumnInfo", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableGroupingColumnInfo extends GroupingColumnInfo {
  private final String columnName;
  private final String groupingTablePath;

  private ImmutableGroupingColumnInfo(String columnName, String groupingTablePath) {
    this.columnName = Objects.requireNonNull(columnName, "columnName");
    this.groupingTablePath = Objects.requireNonNull(groupingTablePath, "groupingTablePath");
  }

  private ImmutableGroupingColumnInfo(
      ImmutableGroupingColumnInfo original,
      String columnName,
      String groupingTablePath) {
    this.columnName = columnName;
    this.groupingTablePath = groupingTablePath;
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
   * @return The relative path name for the column's grouping sidecar table
   */
  @JsonProperty("groupingTablePath")
  @Override
  public String groupingTablePath() {
    return groupingTablePath;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link GroupingColumnInfo#columnName() columnName} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for columnName
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableGroupingColumnInfo withColumnName(String value) {
    String newValue = Objects.requireNonNull(value, "columnName");
    if (this.columnName.equals(newValue)) return this;
    return validate(new ImmutableGroupingColumnInfo(this, newValue, this.groupingTablePath));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link GroupingColumnInfo#groupingTablePath() groupingTablePath} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for groupingTablePath
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableGroupingColumnInfo withGroupingTablePath(String value) {
    String newValue = Objects.requireNonNull(value, "groupingTablePath");
    if (this.groupingTablePath.equals(newValue)) return this;
    return validate(new ImmutableGroupingColumnInfo(this, this.columnName, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableGroupingColumnInfo} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableGroupingColumnInfo
        && equalTo(0, (ImmutableGroupingColumnInfo) another);
  }

  private boolean equalTo(int synthetic, ImmutableGroupingColumnInfo another) {
    return columnName.equals(another.columnName)
        && groupingTablePath.equals(another.groupingTablePath);
  }

  /**
   * Computes a hash code from attributes: {@code columnName}, {@code groupingTablePath}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + columnName.hashCode();
    h += (h << 5) + groupingTablePath.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code GroupingColumnInfo} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("GroupingColumnInfo")
        .omitNullValues()
        .add("columnName", columnName)
        .add("groupingTablePath", groupingTablePath)
        .toString();
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "GroupingColumnInfo", generator = "Immutables")
  @Deprecated
  @SuppressWarnings("Immutable")
  @JsonDeserialize
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends GroupingColumnInfo {
    @Nullable String columnName;
    @Nullable String groupingTablePath;
    @JsonProperty("columnName")
    public void setColumnName(String columnName) {
      this.columnName = columnName;
    }
    @JsonProperty("groupingTablePath")
    public void setGroupingTablePath(String groupingTablePath) {
      this.groupingTablePath = groupingTablePath;
    }
    @Override
    public String columnName() { throw new UnsupportedOperationException(); }
    @Override
    public String groupingTablePath() { throw new UnsupportedOperationException(); }
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static ImmutableGroupingColumnInfo fromJson(Json json) {
    ImmutableGroupingColumnInfo.Builder builder = ImmutableGroupingColumnInfo.builder();
    if (json.columnName != null) {
      builder.columnName(json.columnName);
    }
    if (json.groupingTablePath != null) {
      builder.groupingTablePath(json.groupingTablePath);
    }
    return builder.build();
  }

  /**
   * Construct a new immutable {@code GroupingColumnInfo} instance.
   * @param columnName The value for the {@code columnName} attribute
   * @param groupingTablePath The value for the {@code groupingTablePath} attribute
   * @return An immutable GroupingColumnInfo instance
   */
  public static ImmutableGroupingColumnInfo of(String columnName, String groupingTablePath) {
    return validate(new ImmutableGroupingColumnInfo(columnName, groupingTablePath));
  }

  private static ImmutableGroupingColumnInfo validate(ImmutableGroupingColumnInfo instance) {
    instance.checkGroupingTablePath();
    instance.checkColumnName();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link GroupingColumnInfo} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable GroupingColumnInfo instance
   */
  public static ImmutableGroupingColumnInfo copyOf(GroupingColumnInfo instance) {
    if (instance instanceof ImmutableGroupingColumnInfo) {
      return (ImmutableGroupingColumnInfo) instance;
    }
    return ImmutableGroupingColumnInfo.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableGroupingColumnInfo ImmutableGroupingColumnInfo}.
   * <pre>
   * ImmutableGroupingColumnInfo.builder()
   *    .columnName(String) // required {@link GroupingColumnInfo#columnName() columnName}
   *    .groupingTablePath(String) // required {@link GroupingColumnInfo#groupingTablePath() groupingTablePath}
   *    .build();
   * </pre>
   * @return A new ImmutableGroupingColumnInfo builder
   */
  public static ImmutableGroupingColumnInfo.Builder builder() {
    return new ImmutableGroupingColumnInfo.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableGroupingColumnInfo ImmutableGroupingColumnInfo}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "GroupingColumnInfo", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_COLUMN_NAME = 0x1L;
    private static final long INIT_BIT_GROUPING_TABLE_PATH = 0x2L;
    private long initBits = 0x3L;

    private @Nullable String columnName;
    private @Nullable String groupingTablePath;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code GroupingColumnInfo} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(GroupingColumnInfo instance) {
      Objects.requireNonNull(instance, "instance");
      columnName(instance.columnName());
      groupingTablePath(instance.groupingTablePath());
      return this;
    }

    /**
     * Initializes the value for the {@link GroupingColumnInfo#columnName() columnName} attribute.
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
     * Initializes the value for the {@link GroupingColumnInfo#groupingTablePath() groupingTablePath} attribute.
     * @param groupingTablePath The value for groupingTablePath 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("groupingTablePath")
    public final Builder groupingTablePath(String groupingTablePath) {
      this.groupingTablePath = Objects.requireNonNull(groupingTablePath, "groupingTablePath");
      initBits &= ~INIT_BIT_GROUPING_TABLE_PATH;
      return this;
    }

    /**
     * Builds a new {@link ImmutableGroupingColumnInfo ImmutableGroupingColumnInfo}.
     * @return An immutable instance of GroupingColumnInfo
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableGroupingColumnInfo build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableGroupingColumnInfo.validate(new ImmutableGroupingColumnInfo(null, columnName, groupingTablePath));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_COLUMN_NAME) != 0) attributes.add("columnName");
      if ((initBits & INIT_BIT_GROUPING_TABLE_PATH) != 0) attributes.add("groupingTablePath");
      return "Cannot build GroupingColumnInfo, some of required attributes are not set " + attributes;
    }
  }
}
