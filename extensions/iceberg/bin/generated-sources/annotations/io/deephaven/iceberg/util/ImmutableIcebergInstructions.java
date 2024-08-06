package io.deephaven.iceberg.util;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import io.deephaven.engine.table.TableDefinition;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link IcebergInstructions}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableIcebergInstructions.builder()}.
 */
@Generated(from = "IcebergInstructions", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableIcebergInstructions extends IcebergInstructions {
  private final @Nullable TableDefinition tableDefinition;
  private final @Nullable Object dataInstructions;
  private final ImmutableMap<String, String> columnRenames;

  private ImmutableIcebergInstructions(
      @Nullable TableDefinition tableDefinition,
      @Nullable Object dataInstructions,
      ImmutableMap<String, String> columnRenames) {
    this.tableDefinition = tableDefinition;
    this.dataInstructions = dataInstructions;
    this.columnRenames = columnRenames;
  }

  /**
   * The {@link TableDefinition} to use when reading Iceberg data files.
   */
  @Override
  public Optional<TableDefinition> tableDefinition() {
    return Optional.ofNullable(tableDefinition);
  }

  /**
   * The data instructions to use for reading the Iceberg data files (might be S3Instructions or other cloud
   * provider-specific instructions).
   */
  @Override
  public Optional<Object> dataInstructions() {
    return Optional.ofNullable(dataInstructions);
  }

  /**
   * A {@link Map map} of rename instructions from Iceberg to Deephaven column names to use when reading the Iceberg
   * data files.
   */
  @Override
  public ImmutableMap<String, String> columnRenames() {
    return columnRenames;
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link IcebergInstructions#tableDefinition() tableDefinition} attribute.
   * @param value The value for tableDefinition
   * @return A modified copy of {@code this} object
   */
  public final ImmutableIcebergInstructions withTableDefinition(TableDefinition value) {
    @Nullable TableDefinition newValue = Objects.requireNonNull(value, "tableDefinition");
    if (this.tableDefinition == newValue) return this;
    return new ImmutableIcebergInstructions(newValue, this.dataInstructions, this.columnRenames);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link IcebergInstructions#tableDefinition() tableDefinition} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for tableDefinition
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableIcebergInstructions withTableDefinition(Optional<? extends TableDefinition> optional) {
    @Nullable TableDefinition value = optional.orElse(null);
    if (this.tableDefinition == value) return this;
    return new ImmutableIcebergInstructions(value, this.dataInstructions, this.columnRenames);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link IcebergInstructions#dataInstructions() dataInstructions} attribute.
   * @param value The value for dataInstructions
   * @return A modified copy of {@code this} object
   */
  public final ImmutableIcebergInstructions withDataInstructions(Object value) {
    @Nullable Object newValue = Objects.requireNonNull(value, "dataInstructions");
    if (this.dataInstructions == newValue) return this;
    return new ImmutableIcebergInstructions(this.tableDefinition, newValue, this.columnRenames);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link IcebergInstructions#dataInstructions() dataInstructions} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for dataInstructions
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableIcebergInstructions withDataInstructions(Optional<? extends Object> optional) {
    @Nullable Object value = optional.orElse(null);
    if (this.dataInstructions == value) return this;
    return new ImmutableIcebergInstructions(this.tableDefinition, value, this.columnRenames);
  }

  /**
   * Copy the current immutable object by replacing the {@link IcebergInstructions#columnRenames() columnRenames} map with the specified map.
   * Nulls are not permitted as keys or values.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param entries The entries to be added to the columnRenames map
   * @return A modified copy of {@code this} object
   */
  public final ImmutableIcebergInstructions withColumnRenames(Map<String, ? extends String> entries) {
    if (this.columnRenames == entries) return this;
    ImmutableMap<String, String> newValue = ImmutableMap.copyOf(entries);
    return new ImmutableIcebergInstructions(this.tableDefinition, this.dataInstructions, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableIcebergInstructions} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableIcebergInstructions
        && equalTo(0, (ImmutableIcebergInstructions) another);
  }

  private boolean equalTo(int synthetic, ImmutableIcebergInstructions another) {
    return Objects.equals(tableDefinition, another.tableDefinition)
        && Objects.equals(dataInstructions, another.dataInstructions)
        && columnRenames.equals(another.columnRenames);
  }

  /**
   * Computes a hash code from attributes: {@code tableDefinition}, {@code dataInstructions}, {@code columnRenames}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + Objects.hashCode(tableDefinition);
    h += (h << 5) + Objects.hashCode(dataInstructions);
    h += (h << 5) + columnRenames.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code IcebergInstructions} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("IcebergInstructions")
        .omitNullValues()
        .add("tableDefinition", tableDefinition)
        .add("dataInstructions", dataInstructions)
        .add("columnRenames", columnRenames)
        .toString();
  }

  /**
   * Creates an immutable copy of a {@link IcebergInstructions} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable IcebergInstructions instance
   */
  public static ImmutableIcebergInstructions copyOf(IcebergInstructions instance) {
    if (instance instanceof ImmutableIcebergInstructions) {
      return (ImmutableIcebergInstructions) instance;
    }
    return ImmutableIcebergInstructions.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableIcebergInstructions ImmutableIcebergInstructions}.
   * <pre>
   * ImmutableIcebergInstructions.builder()
   *    .tableDefinition(io.deephaven.engine.table.TableDefinition) // optional {@link IcebergInstructions#tableDefinition() tableDefinition}
   *    .dataInstructions(Object) // optional {@link IcebergInstructions#dataInstructions() dataInstructions}
   *    .putColumnRenames|putAllColumnRenames(String =&gt; String) // {@link IcebergInstructions#columnRenames() columnRenames} mappings
   *    .build();
   * </pre>
   * @return A new ImmutableIcebergInstructions builder
   */
  public static ImmutableIcebergInstructions.Builder builder() {
    return new ImmutableIcebergInstructions.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableIcebergInstructions ImmutableIcebergInstructions}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "IcebergInstructions", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements IcebergInstructions.Builder {
    private @Nullable TableDefinition tableDefinition;
    private @Nullable Object dataInstructions;
    private ImmutableMap.Builder<String, String> columnRenames = ImmutableMap.builder();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code IcebergInstructions} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(IcebergInstructions instance) {
      Objects.requireNonNull(instance, "instance");
      Optional<TableDefinition> tableDefinitionOptional = instance.tableDefinition();
      if (tableDefinitionOptional.isPresent()) {
        tableDefinition(tableDefinitionOptional);
      }
      Optional<Object> dataInstructionsOptional = instance.dataInstructions();
      if (dataInstructionsOptional.isPresent()) {
        dataInstructions(dataInstructionsOptional);
      }
      putAllColumnRenames(instance.columnRenames());
      return this;
    }

    /**
     * Initializes the optional value {@link IcebergInstructions#tableDefinition() tableDefinition} to tableDefinition.
     * @param tableDefinition The value for tableDefinition
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder tableDefinition(TableDefinition tableDefinition) {
      this.tableDefinition = Objects.requireNonNull(tableDefinition, "tableDefinition");
      return this;
    }

    /**
     * Initializes the optional value {@link IcebergInstructions#tableDefinition() tableDefinition} to tableDefinition.
     * @param tableDefinition The value for tableDefinition
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder tableDefinition(Optional<? extends TableDefinition> tableDefinition) {
      this.tableDefinition = tableDefinition.orElse(null);
      return this;
    }

    /**
     * Initializes the optional value {@link IcebergInstructions#dataInstructions() dataInstructions} to dataInstructions.
     * @param dataInstructions The value for dataInstructions
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder dataInstructions(Object dataInstructions) {
      this.dataInstructions = Objects.requireNonNull(dataInstructions, "dataInstructions");
      return this;
    }

    /**
     * Initializes the optional value {@link IcebergInstructions#dataInstructions() dataInstructions} to dataInstructions.
     * @param dataInstructions The value for dataInstructions
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder dataInstructions(Optional<? extends Object> dataInstructions) {
      this.dataInstructions = dataInstructions.orElse(null);
      return this;
    }

    /**
     * Put one entry to the {@link IcebergInstructions#columnRenames() columnRenames} map.
     * @param key The key in the columnRenames map
     * @param value The associated value in the columnRenames map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder putColumnRenames(String key, String value) {
      this.columnRenames.put(key, value);
      return this;
    }

    /**
     * Put one entry to the {@link IcebergInstructions#columnRenames() columnRenames} map. Nulls are not permitted
     * @param entry The key and value entry
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder putColumnRenames(Map.Entry<String, ? extends String> entry) {
      this.columnRenames.put(entry);
      return this;
    }

    /**
     * Sets or replaces all mappings from the specified map as entries for the {@link IcebergInstructions#columnRenames() columnRenames} map. Nulls are not permitted
     * @param entries The entries that will be added to the columnRenames map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder columnRenames(Map<String, ? extends String> entries) {
      this.columnRenames = ImmutableMap.builder();
      return putAllColumnRenames(entries);
    }

    /**
     * Put all mappings from the specified map as entries to {@link IcebergInstructions#columnRenames() columnRenames} map. Nulls are not permitted
     * @param entries The entries that will be added to the columnRenames map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder putAllColumnRenames(Map<String, ? extends String> entries) {
      this.columnRenames.putAll(entries);
      return this;
    }

    /**
     * Builds a new {@link ImmutableIcebergInstructions ImmutableIcebergInstructions}.
     * @return An immutable instance of IcebergInstructions
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableIcebergInstructions build() {
      return new ImmutableIcebergInstructions(tableDefinition, dataInstructions, columnRenames.build());
    }
  }
}
