package io.deephaven.parquet.table.metadata;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link DataIndexInfo}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableDataIndexInfo.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableDataIndexInfo.of()}.
 */
@Generated(from = "DataIndexInfo", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableDataIndexInfo extends DataIndexInfo {
  private final ImmutableSet<String> columns;
  private final String indexTablePath;

  private ImmutableDataIndexInfo(Iterable<String> columns, String indexTablePath) {
    this.columns = ImmutableSet.copyOf(columns);
    this.indexTablePath = Objects.requireNonNull(indexTablePath, "indexTablePath");
  }

  private ImmutableDataIndexInfo(
      ImmutableDataIndexInfo original,
      ImmutableSet<String> columns,
      String indexTablePath) {
    this.columns = columns;
    this.indexTablePath = indexTablePath;
  }

  /**
   * @return The column names
   */
  @JsonProperty("columns")
  @Override
  public ImmutableSet<String> columns() {
    return columns;
  }

  /**
   * @return The relative path name for the columns' data index sidecar table
   */
  @JsonProperty("indexTablePath")
  @Override
  public String indexTablePath() {
    return indexTablePath;
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link DataIndexInfo#columns() columns}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableDataIndexInfo withColumns(String... elements) {
    ImmutableSet<String> newValue = ImmutableSet.copyOf(elements);
    return validate(new ImmutableDataIndexInfo(this, newValue, this.indexTablePath));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link DataIndexInfo#columns() columns}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of columns elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableDataIndexInfo withColumns(Iterable<String> elements) {
    if (this.columns == elements) return this;
    ImmutableSet<String> newValue = ImmutableSet.copyOf(elements);
    return validate(new ImmutableDataIndexInfo(this, newValue, this.indexTablePath));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DataIndexInfo#indexTablePath() indexTablePath} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for indexTablePath
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDataIndexInfo withIndexTablePath(String value) {
    String newValue = Objects.requireNonNull(value, "indexTablePath");
    if (this.indexTablePath.equals(newValue)) return this;
    return validate(new ImmutableDataIndexInfo(this, this.columns, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableDataIndexInfo} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableDataIndexInfo
        && equalTo(0, (ImmutableDataIndexInfo) another);
  }

  private boolean equalTo(int synthetic, ImmutableDataIndexInfo another) {
    return columns.equals(another.columns)
        && indexTablePath.equals(another.indexTablePath);
  }

  /**
   * Computes a hash code from attributes: {@code columns}, {@code indexTablePath}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + columns.hashCode();
    h += (h << 5) + indexTablePath.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code DataIndexInfo} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("DataIndexInfo")
        .omitNullValues()
        .add("columns", columns)
        .add("indexTablePath", indexTablePath)
        .toString();
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "DataIndexInfo", generator = "Immutables")
  @Deprecated
  @SuppressWarnings("Immutable")
  @JsonDeserialize
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends DataIndexInfo {
    @Nullable Set<String> columns = ImmutableSet.of();
    @Nullable String indexTablePath;
    @JsonProperty("columns")
    public void setColumns(Set<String> columns) {
      this.columns = columns;
    }
    @JsonProperty("indexTablePath")
    public void setIndexTablePath(String indexTablePath) {
      this.indexTablePath = indexTablePath;
    }
    @Override
    public Set<String> columns() { throw new UnsupportedOperationException(); }
    @Override
    public String indexTablePath() { throw new UnsupportedOperationException(); }
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static ImmutableDataIndexInfo fromJson(Json json) {
    ImmutableDataIndexInfo.Builder builder = ImmutableDataIndexInfo.builder();
    if (json.columns != null) {
      builder.addAllColumns(json.columns);
    }
    if (json.indexTablePath != null) {
      builder.indexTablePath(json.indexTablePath);
    }
    return builder.build();
  }

  /**
   * Construct a new immutable {@code DataIndexInfo} instance.
   * @param columns The value for the {@code columns} attribute
   * @param indexTablePath The value for the {@code indexTablePath} attribute
   * @return An immutable DataIndexInfo instance
   */
  public static ImmutableDataIndexInfo of(Set<String> columns, String indexTablePath) {
    return of((Iterable<String>) columns, indexTablePath);
  }

  /**
   * Construct a new immutable {@code DataIndexInfo} instance.
   * @param columns The value for the {@code columns} attribute
   * @param indexTablePath The value for the {@code indexTablePath} attribute
   * @return An immutable DataIndexInfo instance
   */
  public static ImmutableDataIndexInfo of(Iterable<String> columns, String indexTablePath) {
    return validate(new ImmutableDataIndexInfo(columns, indexTablePath));
  }

  private static ImmutableDataIndexInfo validate(ImmutableDataIndexInfo instance) {
    instance.checkIndexTablePath();
    instance.checkColumns();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link DataIndexInfo} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable DataIndexInfo instance
   */
  public static ImmutableDataIndexInfo copyOf(DataIndexInfo instance) {
    if (instance instanceof ImmutableDataIndexInfo) {
      return (ImmutableDataIndexInfo) instance;
    }
    return ImmutableDataIndexInfo.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableDataIndexInfo ImmutableDataIndexInfo}.
   * <pre>
   * ImmutableDataIndexInfo.builder()
   *    .addColumns|addAllColumns(String) // {@link DataIndexInfo#columns() columns} elements
   *    .indexTablePath(String) // required {@link DataIndexInfo#indexTablePath() indexTablePath}
   *    .build();
   * </pre>
   * @return A new ImmutableDataIndexInfo builder
   */
  public static ImmutableDataIndexInfo.Builder builder() {
    return new ImmutableDataIndexInfo.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableDataIndexInfo ImmutableDataIndexInfo}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "DataIndexInfo", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_INDEX_TABLE_PATH = 0x1L;
    private long initBits = 0x1L;

    private ImmutableSet.Builder<String> columns = ImmutableSet.builder();
    private @Nullable String indexTablePath;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code DataIndexInfo} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(DataIndexInfo instance) {
      Objects.requireNonNull(instance, "instance");
      addAllColumns(instance.columns());
      indexTablePath(instance.indexTablePath());
      return this;
    }

    /**
     * Adds one element to {@link DataIndexInfo#columns() columns} set.
     * @param element A columns element
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addColumns(String element) {
      this.columns.add(element);
      return this;
    }

    /**
     * Adds elements to {@link DataIndexInfo#columns() columns} set.
     * @param elements An array of columns elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addColumns(String... elements) {
      this.columns.add(elements);
      return this;
    }


    /**
     * Sets or replaces all elements for {@link DataIndexInfo#columns() columns} set.
     * @param elements An iterable of columns elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("columns")
    public final Builder columns(Iterable<String> elements) {
      this.columns = ImmutableSet.builder();
      return addAllColumns(elements);
    }

    /**
     * Adds elements to {@link DataIndexInfo#columns() columns} set.
     * @param elements An iterable of columns elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addAllColumns(Iterable<String> elements) {
      this.columns.addAll(elements);
      return this;
    }

    /**
     * Initializes the value for the {@link DataIndexInfo#indexTablePath() indexTablePath} attribute.
     * @param indexTablePath The value for indexTablePath 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("indexTablePath")
    public final Builder indexTablePath(String indexTablePath) {
      this.indexTablePath = Objects.requireNonNull(indexTablePath, "indexTablePath");
      initBits &= ~INIT_BIT_INDEX_TABLE_PATH;
      return this;
    }

    /**
     * Builds a new {@link ImmutableDataIndexInfo ImmutableDataIndexInfo}.
     * @return An immutable instance of DataIndexInfo
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableDataIndexInfo build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableDataIndexInfo.validate(new ImmutableDataIndexInfo(null, columns.build(), indexTablePath));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_INDEX_TABLE_PATH) != 0) attributes.add("indexTablePath");
      return "Cannot build DataIndexInfo, some of required attributes are not set " + attributes;
    }
  }
}
