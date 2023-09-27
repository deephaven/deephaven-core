package io.deephaven.engine.table;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link MultiJoinInput}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableMultiJoinInput.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableMultiJoinInput.of()}.
 */
@Generated(from = "MultiJoinInput", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableMultiJoinInput extends MultiJoinInput {
  private final Table inputTable;
  private final JoinMatch[] columnsToMatch;
  private final JoinAddition[] columnsToAdd;

  private ImmutableMultiJoinInput(
      Table inputTable,
      JoinMatch[] columnsToMatch,
      JoinAddition[] columnsToAdd) {
    this.inputTable = Objects.requireNonNull(inputTable, "inputTable");
    this.columnsToMatch = columnsToMatch.clone();
    this.columnsToAdd = columnsToAdd.clone();
  }

  private ImmutableMultiJoinInput(
      ImmutableMultiJoinInput original,
      Table inputTable,
      JoinMatch[] columnsToMatch,
      JoinAddition[] columnsToAdd) {
    this.inputTable = inputTable;
    this.columnsToMatch = columnsToMatch;
    this.columnsToAdd = columnsToAdd;
  }

  /**
   * @return The value of the {@code inputTable} attribute
   */
  @Override
  public Table inputTable() {
    return inputTable;
  }

  /**
   * @return A cloned {@code columnsToMatch} array
   */
  @Override
  public JoinMatch[] columnsToMatch() {
    return columnsToMatch.clone();
  }

  /**
   * @return A cloned {@code columnsToAdd} array
   */
  @Override
  public JoinAddition[] columnsToAdd() {
    return columnsToAdd.clone();
  }

  /**
   * Copy the current immutable object by setting a value for the {@link MultiJoinInput#inputTable() inputTable} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for inputTable
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableMultiJoinInput withInputTable(Table value) {
    if (this.inputTable == value) return this;
    Table newValue = Objects.requireNonNull(value, "inputTable");
    return new ImmutableMultiJoinInput(this, newValue, this.columnsToMatch, this.columnsToAdd);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link MultiJoinInput#columnsToMatch() columnsToMatch}.
   * The array is cloned before being saved as attribute values.
   * @param elements The non-null elements for columnsToMatch
   * @return A modified copy of {@code this} object
   */
  public final ImmutableMultiJoinInput withColumnsToMatch(JoinMatch... elements) {
    JoinMatch[] newValue = elements.clone();
    return new ImmutableMultiJoinInput(this, this.inputTable, newValue, this.columnsToAdd);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link MultiJoinInput#columnsToAdd() columnsToAdd}.
   * The array is cloned before being saved as attribute values.
   * @param elements The non-null elements for columnsToAdd
   * @return A modified copy of {@code this} object
   */
  public final ImmutableMultiJoinInput withColumnsToAdd(JoinAddition... elements) {
    JoinAddition[] newValue = elements.clone();
    return new ImmutableMultiJoinInput(this, this.inputTable, this.columnsToMatch, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableMultiJoinInput} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableMultiJoinInput
        && equalTo(0, (ImmutableMultiJoinInput) another);
  }

  private boolean equalTo(int synthetic, ImmutableMultiJoinInput another) {
    return inputTable.equals(another.inputTable)
        && Arrays.equals(columnsToMatch, another.columnsToMatch)
        && Arrays.equals(columnsToAdd, another.columnsToAdd);
  }

  /**
   * Computes a hash code from attributes: {@code inputTable}, {@code columnsToMatch}, {@code columnsToAdd}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + inputTable.hashCode();
    h += (h << 5) + Arrays.hashCode(columnsToMatch);
    h += (h << 5) + Arrays.hashCode(columnsToAdd);
    return h;
  }

  /**
   * Prints the immutable value {@code MultiJoinInput} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("MultiJoinInput")
        .omitNullValues()
        .add("inputTable", inputTable)
        .add("columnsToMatch", Arrays.toString(columnsToMatch))
        .add("columnsToAdd", Arrays.toString(columnsToAdd))
        .toString();
  }

  /**
   * Construct a new immutable {@code MultiJoinInput} instance.
   * @param inputTable The value for the {@code inputTable} attribute
   * @param columnsToMatch The value for the {@code columnsToMatch} attribute
   * @param columnsToAdd The value for the {@code columnsToAdd} attribute
   * @return An immutable MultiJoinInput instance
   */
  public static ImmutableMultiJoinInput of(Table inputTable, JoinMatch[] columnsToMatch, JoinAddition[] columnsToAdd) {
    return new ImmutableMultiJoinInput(inputTable, columnsToMatch, columnsToAdd);
  }

  /**
   * Creates an immutable copy of a {@link MultiJoinInput} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable MultiJoinInput instance
   */
  public static ImmutableMultiJoinInput copyOf(MultiJoinInput instance) {
    if (instance instanceof ImmutableMultiJoinInput) {
      return (ImmutableMultiJoinInput) instance;
    }
    return ImmutableMultiJoinInput.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableMultiJoinInput ImmutableMultiJoinInput}.
   * <pre>
   * ImmutableMultiJoinInput.builder()
   *    .inputTable(io.deephaven.engine.table.Table) // required {@link MultiJoinInput#inputTable() inputTable}
   *    .columnsToMatch(io.deephaven.api.JoinMatch) // required {@link MultiJoinInput#columnsToMatch() columnsToMatch}
   *    .columnsToAdd(io.deephaven.api.JoinAddition) // required {@link MultiJoinInput#columnsToAdd() columnsToAdd}
   *    .build();
   * </pre>
   * @return A new ImmutableMultiJoinInput builder
   */
  public static ImmutableMultiJoinInput.Builder builder() {
    return new ImmutableMultiJoinInput.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableMultiJoinInput ImmutableMultiJoinInput}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "MultiJoinInput", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_INPUT_TABLE = 0x1L;
    private static final long INIT_BIT_COLUMNS_TO_MATCH = 0x2L;
    private static final long INIT_BIT_COLUMNS_TO_ADD = 0x4L;
    private long initBits = 0x7L;

    private @Nullable Table inputTable;
    private @Nullable JoinMatch[] columnsToMatch;
    private @Nullable JoinAddition[] columnsToAdd;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code MultiJoinInput} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(MultiJoinInput instance) {
      Objects.requireNonNull(instance, "instance");
      inputTable(instance.inputTable());
      columnsToMatch(instance.columnsToMatch());
      columnsToAdd(instance.columnsToAdd());
      return this;
    }

    /**
     * Initializes the value for the {@link MultiJoinInput#inputTable() inputTable} attribute.
     * @param inputTable The value for inputTable 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder inputTable(Table inputTable) {
      this.inputTable = Objects.requireNonNull(inputTable, "inputTable");
      initBits &= ~INIT_BIT_INPUT_TABLE;
      return this;
    }

    /**
     * Initializes the value for the {@link MultiJoinInput#columnsToMatch() columnsToMatch} attribute.
     * @param columnsToMatch The elements for columnsToMatch
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder columnsToMatch(JoinMatch... columnsToMatch) {
      this.columnsToMatch = columnsToMatch.clone();
      initBits &= ~INIT_BIT_COLUMNS_TO_MATCH;
      return this;
    }

    /**
     * Initializes the value for the {@link MultiJoinInput#columnsToAdd() columnsToAdd} attribute.
     * @param columnsToAdd The elements for columnsToAdd
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder columnsToAdd(JoinAddition... columnsToAdd) {
      this.columnsToAdd = columnsToAdd.clone();
      initBits &= ~INIT_BIT_COLUMNS_TO_ADD;
      return this;
    }

    /**
     * Builds a new {@link ImmutableMultiJoinInput ImmutableMultiJoinInput}.
     * @return An immutable instance of MultiJoinInput
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableMultiJoinInput build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableMultiJoinInput(null, inputTable, columnsToMatch, columnsToAdd);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_INPUT_TABLE) != 0) attributes.add("inputTable");
      if ((initBits & INIT_BIT_COLUMNS_TO_MATCH) != 0) attributes.add("columnsToMatch");
      if ((initBits & INIT_BIT_COLUMNS_TO_ADD) != 0) attributes.add("columnsToAdd");
      return "Cannot build MultiJoinInput, some of required attributes are not set " + attributes;
    }
  }
}
