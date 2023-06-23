package io.deephaven.parquet.table.metadata;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.List;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link TableInfo}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableTableInfo.builder()}.
 */
@Generated(from = "TableInfo", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableTableInfo extends TableInfo {
  private final String version;
  private final ImmutableList<GroupingColumnInfo> groupingColumns;
  private final ImmutableList<ColumnTypeInfo> columnTypes;

  private ImmutableTableInfo(ImmutableTableInfo.Builder builder) {
    this.groupingColumns = builder.groupingColumns.build();
    this.columnTypes = builder.columnTypes.build();
    this.version = builder.version != null
        ? builder.version
        : Objects.requireNonNull(super.version(), "version");
  }

  private ImmutableTableInfo(
      String version,
      ImmutableList<GroupingColumnInfo> groupingColumns,
      ImmutableList<ColumnTypeInfo> columnTypes) {
    this.version = version;
    this.groupingColumns = groupingColumns;
    this.columnTypes = columnTypes;
  }

  /**
   * @return The Deephaven release version when this metadata format was defined
   */
  @JsonProperty("version")
  @Override
  public String version() {
    return version;
  }

  /**
   * @return List of {@link GroupingColumnInfo grouping columns} for columns with grouped data
   */
  @JsonProperty("groupingColumns")
  @Override
  public ImmutableList<GroupingColumnInfo> groupingColumns() {
    return groupingColumns;
  }

  /**
   * @return List of {@link ColumnTypeInfo column types} for columns requiring non-default deserialization or type
   *         selection
   */
  @JsonProperty("columnTypes")
  @Override
  public ImmutableList<ColumnTypeInfo> columnTypes() {
    return columnTypes;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TableInfo#version() version} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for version
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTableInfo withVersion(String value) {
    String newValue = Objects.requireNonNull(value, "version");
    if (this.version.equals(newValue)) return this;
    return validate(new ImmutableTableInfo(newValue, this.groupingColumns, this.columnTypes));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link TableInfo#groupingColumns() groupingColumns}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableTableInfo withGroupingColumns(GroupingColumnInfo... elements) {
    ImmutableList<GroupingColumnInfo> newValue = ImmutableList.copyOf(elements);
    return validate(new ImmutableTableInfo(this.version, newValue, this.columnTypes));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link TableInfo#groupingColumns() groupingColumns}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of groupingColumns elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableTableInfo withGroupingColumns(Iterable<? extends GroupingColumnInfo> elements) {
    if (this.groupingColumns == elements) return this;
    ImmutableList<GroupingColumnInfo> newValue = ImmutableList.copyOf(elements);
    return validate(new ImmutableTableInfo(this.version, newValue, this.columnTypes));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link TableInfo#columnTypes() columnTypes}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableTableInfo withColumnTypes(ColumnTypeInfo... elements) {
    ImmutableList<ColumnTypeInfo> newValue = ImmutableList.copyOf(elements);
    return validate(new ImmutableTableInfo(this.version, this.groupingColumns, newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link TableInfo#columnTypes() columnTypes}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of columnTypes elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableTableInfo withColumnTypes(Iterable<? extends ColumnTypeInfo> elements) {
    if (this.columnTypes == elements) return this;
    ImmutableList<ColumnTypeInfo> newValue = ImmutableList.copyOf(elements);
    return validate(new ImmutableTableInfo(this.version, this.groupingColumns, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableTableInfo} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableTableInfo
        && equalTo(0, (ImmutableTableInfo) another);
  }

  private boolean equalTo(int synthetic, ImmutableTableInfo another) {
    return version.equals(another.version)
        && groupingColumns.equals(another.groupingColumns)
        && columnTypes.equals(another.columnTypes);
  }

  /**
   * Computes a hash code from attributes: {@code version}, {@code groupingColumns}, {@code columnTypes}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + version.hashCode();
    h += (h << 5) + groupingColumns.hashCode();
    h += (h << 5) + columnTypes.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code TableInfo} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("TableInfo")
        .omitNullValues()
        .add("version", version)
        .add("groupingColumns", groupingColumns)
        .add("columnTypes", columnTypes)
        .toString();
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "TableInfo", generator = "Immutables")
  @Deprecated
  @SuppressWarnings("Immutable")
  @JsonDeserialize
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends TableInfo {
    @Nullable String version;
    @Nullable List<GroupingColumnInfo> groupingColumns = ImmutableList.of();
    @Nullable List<ColumnTypeInfo> columnTypes = ImmutableList.of();
    @JsonProperty("version")
    public void setVersion(String version) {
      this.version = version;
    }
    @JsonProperty("groupingColumns")
    public void setGroupingColumns(List<GroupingColumnInfo> groupingColumns) {
      this.groupingColumns = groupingColumns;
    }
    @JsonProperty("columnTypes")
    public void setColumnTypes(List<ColumnTypeInfo> columnTypes) {
      this.columnTypes = columnTypes;
    }
    @Override
    public String version() { throw new UnsupportedOperationException(); }
    @Override
    public List<GroupingColumnInfo> groupingColumns() { throw new UnsupportedOperationException(); }
    @Override
    public List<ColumnTypeInfo> columnTypes() { throw new UnsupportedOperationException(); }
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static ImmutableTableInfo fromJson(Json json) {
    ImmutableTableInfo.Builder builder = ImmutableTableInfo.builder();
    if (json.version != null) {
      builder.version(json.version);
    }
    if (json.groupingColumns != null) {
      builder.addAllGroupingColumns(json.groupingColumns);
    }
    if (json.columnTypes != null) {
      builder.addAllColumnTypes(json.columnTypes);
    }
    return builder.build();
  }

  private static ImmutableTableInfo validate(ImmutableTableInfo instance) {
    instance.checkVersion();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link TableInfo} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable TableInfo instance
   */
  public static ImmutableTableInfo copyOf(TableInfo instance) {
    if (instance instanceof ImmutableTableInfo) {
      return (ImmutableTableInfo) instance;
    }
    return ImmutableTableInfo.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableTableInfo ImmutableTableInfo}.
   * <pre>
   * ImmutableTableInfo.builder()
   *    .version(String) // optional {@link TableInfo#version() version}
   *    .addGroupingColumns|addAllGroupingColumns(io.deephaven.parquet.table.metadata.GroupingColumnInfo) // {@link TableInfo#groupingColumns() groupingColumns} elements
   *    .addColumnTypes|addAllColumnTypes(io.deephaven.parquet.table.metadata.ColumnTypeInfo) // {@link TableInfo#columnTypes() columnTypes} elements
   *    .build();
   * </pre>
   * @return A new ImmutableTableInfo builder
   */
  public static ImmutableTableInfo.Builder builder() {
    return new ImmutableTableInfo.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableTableInfo ImmutableTableInfo}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "TableInfo", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements TableInfo.Builder {
    private @Nullable String version;
    private ImmutableList.Builder<GroupingColumnInfo> groupingColumns = ImmutableList.builder();
    private ImmutableList.Builder<ColumnTypeInfo> columnTypes = ImmutableList.builder();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code TableInfo} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(TableInfo instance) {
      Objects.requireNonNull(instance, "instance");
      version(instance.version());
      addAllGroupingColumns(instance.groupingColumns());
      addAllColumnTypes(instance.columnTypes());
      return this;
    }

    /**
     * Initializes the value for the {@link TableInfo#version() version} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TableInfo#version() version}.</em>
     * @param version The value for version 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("version")
    public final Builder version(String version) {
      this.version = Objects.requireNonNull(version, "version");
      return this;
    }

    /**
     * Adds one element to {@link TableInfo#groupingColumns() groupingColumns} list.
     * @param element A groupingColumns element
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addGroupingColumns(GroupingColumnInfo element) {
      this.groupingColumns.add(element);
      return this;
    }

    /**
     * Adds elements to {@link TableInfo#groupingColumns() groupingColumns} list.
     * @param elements An array of groupingColumns elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addGroupingColumns(GroupingColumnInfo... elements) {
      this.groupingColumns.add(elements);
      return this;
    }


    /**
     * Sets or replaces all elements for {@link TableInfo#groupingColumns() groupingColumns} list.
     * @param elements An iterable of groupingColumns elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("groupingColumns")
    public final Builder groupingColumns(Iterable<? extends GroupingColumnInfo> elements) {
      this.groupingColumns = ImmutableList.builder();
      return addAllGroupingColumns(elements);
    }

    /**
     * Adds elements to {@link TableInfo#groupingColumns() groupingColumns} list.
     * @param elements An iterable of groupingColumns elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addAllGroupingColumns(Iterable<? extends GroupingColumnInfo> elements) {
      this.groupingColumns.addAll(elements);
      return this;
    }

    /**
     * Adds one element to {@link TableInfo#columnTypes() columnTypes} list.
     * @param element A columnTypes element
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addColumnTypes(ColumnTypeInfo element) {
      this.columnTypes.add(element);
      return this;
    }

    /**
     * Adds elements to {@link TableInfo#columnTypes() columnTypes} list.
     * @param elements An array of columnTypes elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addColumnTypes(ColumnTypeInfo... elements) {
      this.columnTypes.add(elements);
      return this;
    }


    /**
     * Sets or replaces all elements for {@link TableInfo#columnTypes() columnTypes} list.
     * @param elements An iterable of columnTypes elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("columnTypes")
    public final Builder columnTypes(Iterable<? extends ColumnTypeInfo> elements) {
      this.columnTypes = ImmutableList.builder();
      return addAllColumnTypes(elements);
    }

    /**
     * Adds elements to {@link TableInfo#columnTypes() columnTypes} list.
     * @param elements An iterable of columnTypes elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addAllColumnTypes(Iterable<? extends ColumnTypeInfo> elements) {
      this.columnTypes.addAll(elements);
      return this;
    }

    /**
     * Builds a new {@link ImmutableTableInfo ImmutableTableInfo}.
     * @return An immutable instance of TableInfo
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableTableInfo build() {
      return ImmutableTableInfo.validate(new ImmutableTableInfo(this));
    }
  }
}
