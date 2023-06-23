package io.deephaven.sql;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ScopeStaticImpl}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableScopeStaticImpl.builder()}.
 */
@Generated(from = "ScopeStaticImpl", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableScopeStaticImpl extends ScopeStaticImpl {
  private final ImmutableList<TableInformation> tables;

  private ImmutableScopeStaticImpl(ImmutableList<TableInformation> tables) {
    this.tables = tables;
  }

  /**
   * @return The value of the {@code tables} attribute
   */
  @Override
  public ImmutableList<TableInformation> tables() {
    return tables;
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ScopeStaticImpl#tables() tables}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableScopeStaticImpl withTables(TableInformation... elements) {
    ImmutableList<TableInformation> newValue = ImmutableList.copyOf(elements);
    return validate(new ImmutableScopeStaticImpl(newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ScopeStaticImpl#tables() tables}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of tables elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableScopeStaticImpl withTables(Iterable<? extends TableInformation> elements) {
    if (this.tables == elements) return this;
    ImmutableList<TableInformation> newValue = ImmutableList.copyOf(elements);
    return validate(new ImmutableScopeStaticImpl(newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableScopeStaticImpl} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableScopeStaticImpl
        && equalTo(0, (ImmutableScopeStaticImpl) another);
  }

  private boolean equalTo(int synthetic, ImmutableScopeStaticImpl another) {
    return tables.equals(another.tables);
  }

  /**
   * Computes a hash code from attributes: {@code tables}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + tables.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code ScopeStaticImpl} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("ScopeStaticImpl")
        .omitNullValues()
        .add("tables", tables)
        .toString();
  }

  private static ImmutableScopeStaticImpl validate(ImmutableScopeStaticImpl instance) {
    instance.checkUniqueQualifiedNames();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link ScopeStaticImpl} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ScopeStaticImpl instance
   */
  public static ImmutableScopeStaticImpl copyOf(ScopeStaticImpl instance) {
    if (instance instanceof ImmutableScopeStaticImpl) {
      return (ImmutableScopeStaticImpl) instance;
    }
    return ImmutableScopeStaticImpl.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableScopeStaticImpl ImmutableScopeStaticImpl}.
   * <pre>
   * ImmutableScopeStaticImpl.builder()
   *    .addTables|addAllTables(io.deephaven.sql.TableInformation) // {@link ScopeStaticImpl#tables() tables} elements
   *    .build();
   * </pre>
   * @return A new ImmutableScopeStaticImpl builder
   */
  public static ImmutableScopeStaticImpl.Builder builder() {
    return new ImmutableScopeStaticImpl.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableScopeStaticImpl ImmutableScopeStaticImpl}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ScopeStaticImpl", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements ScopeStaticImpl.Builder {
    private ImmutableList.Builder<TableInformation> tables = ImmutableList.builder();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ScopeStaticImpl} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(ScopeStaticImpl instance) {
      Objects.requireNonNull(instance, "instance");
      addAllTables(instance.tables());
      return this;
    }

    /**
     * Adds one element to {@link ScopeStaticImpl#tables() tables} list.
     * @param element A tables element
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addTables(TableInformation element) {
      this.tables.add(element);
      return this;
    }

    /**
     * Adds elements to {@link ScopeStaticImpl#tables() tables} list.
     * @param elements An array of tables elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addTables(TableInformation... elements) {
      this.tables.add(elements);
      return this;
    }


    /**
     * Sets or replaces all elements for {@link ScopeStaticImpl#tables() tables} list.
     * @param elements An iterable of tables elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder tables(Iterable<? extends TableInformation> elements) {
      this.tables = ImmutableList.builder();
      return addAllTables(elements);
    }

    /**
     * Adds elements to {@link ScopeStaticImpl#tables() tables} list.
     * @param elements An iterable of tables elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addAllTables(Iterable<? extends TableInformation> elements) {
      this.tables.addAll(elements);
      return this;
    }

    /**
     * Builds a new {@link ImmutableScopeStaticImpl ImmutableScopeStaticImpl}.
     * @return An immutable instance of ScopeStaticImpl
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableScopeStaticImpl build() {
      return ImmutableScopeStaticImpl.validate(new ImmutableScopeStaticImpl(tables.build()));
    }
  }
}
