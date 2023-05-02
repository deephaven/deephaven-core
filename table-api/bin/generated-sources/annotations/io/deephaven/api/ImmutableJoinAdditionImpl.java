package io.deephaven.api;

import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link JoinAdditionImpl}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableJoinAdditionImpl.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableJoinAdditionImpl.of()}.
 */
@Generated(from = "JoinAdditionImpl", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
final class ImmutableJoinAdditionImpl extends JoinAdditionImpl {
  private final ColumnName newColumn;
  private final ColumnName existingColumn;

  private ImmutableJoinAdditionImpl(ColumnName newColumn, ColumnName existingColumn) {
    this.newColumn = Objects.requireNonNull(newColumn, "newColumn");
    this.existingColumn = Objects.requireNonNull(existingColumn, "existingColumn");
  }

  private ImmutableJoinAdditionImpl(
      ImmutableJoinAdditionImpl original,
      ColumnName newColumn,
      ColumnName existingColumn) {
    this.newColumn = newColumn;
    this.existingColumn = existingColumn;
  }

  /**
   * @return The value of the {@code newColumn} attribute
   */
  @Override
  public ColumnName newColumn() {
    return newColumn;
  }

  /**
   * @return The value of the {@code existingColumn} attribute
   */
  @Override
  public ColumnName existingColumn() {
    return existingColumn;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JoinAdditionImpl#newColumn() newColumn} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for newColumn
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJoinAdditionImpl withNewColumn(ColumnName value) {
    if (this.newColumn == value) return this;
    ColumnName newValue = Objects.requireNonNull(value, "newColumn");
    return validate(new ImmutableJoinAdditionImpl(this, newValue, this.existingColumn));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JoinAdditionImpl#existingColumn() existingColumn} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for existingColumn
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJoinAdditionImpl withExistingColumn(ColumnName value) {
    if (this.existingColumn == value) return this;
    ColumnName newValue = Objects.requireNonNull(value, "existingColumn");
    return validate(new ImmutableJoinAdditionImpl(this, this.newColumn, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableJoinAdditionImpl} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableJoinAdditionImpl
        && equalTo(0, (ImmutableJoinAdditionImpl) another);
  }

  private boolean equalTo(int synthetic, ImmutableJoinAdditionImpl another) {
    return newColumn.equals(another.newColumn)
        && existingColumn.equals(another.existingColumn);
  }

  /**
   * Computes a hash code from attributes: {@code newColumn}, {@code existingColumn}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + newColumn.hashCode();
    h += (h << 5) + existingColumn.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code JoinAdditionImpl} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "JoinAdditionImpl{"
        + "newColumn=" + newColumn
        + ", existingColumn=" + existingColumn
        + "}";
  }

  /**
   * Construct a new immutable {@code JoinAdditionImpl} instance.
   * @param newColumn The value for the {@code newColumn} attribute
   * @param existingColumn The value for the {@code existingColumn} attribute
   * @return An immutable JoinAdditionImpl instance
   */
  public static ImmutableJoinAdditionImpl of(ColumnName newColumn, ColumnName existingColumn) {
    return validate(new ImmutableJoinAdditionImpl(newColumn, existingColumn));
  }

  private static ImmutableJoinAdditionImpl validate(ImmutableJoinAdditionImpl instance) {
    instance.checkNotSameColumn();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link JoinAdditionImpl} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable JoinAdditionImpl instance
   */
  public static ImmutableJoinAdditionImpl copyOf(JoinAdditionImpl instance) {
    if (instance instanceof ImmutableJoinAdditionImpl) {
      return (ImmutableJoinAdditionImpl) instance;
    }
    return ImmutableJoinAdditionImpl.builder()
        .from(instance)
        .build();
  }

  private Object readResolve() throws ObjectStreamException {
    return validate(this);
  }

  /**
   * Creates a builder for {@link ImmutableJoinAdditionImpl ImmutableJoinAdditionImpl}.
   * <pre>
   * ImmutableJoinAdditionImpl.builder()
   *    .newColumn(io.deephaven.api.ColumnName) // required {@link JoinAdditionImpl#newColumn() newColumn}
   *    .existingColumn(io.deephaven.api.ColumnName) // required {@link JoinAdditionImpl#existingColumn() existingColumn}
   *    .build();
   * </pre>
   * @return A new ImmutableJoinAdditionImpl builder
   */
  public static ImmutableJoinAdditionImpl.Builder builder() {
    return new ImmutableJoinAdditionImpl.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableJoinAdditionImpl ImmutableJoinAdditionImpl}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "JoinAdditionImpl", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_NEW_COLUMN = 0x1L;
    private static final long INIT_BIT_EXISTING_COLUMN = 0x2L;
    private long initBits = 0x3L;

    private @Nullable ColumnName newColumn;
    private @Nullable ColumnName existingColumn;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.api.JoinAdditionImpl} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(JoinAdditionImpl instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.api.JoinAddition} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(JoinAddition instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    private void from(Object object) {
      long bits = 0;
      if (object instanceof JoinAdditionImpl) {
        JoinAdditionImpl instance = (JoinAdditionImpl) object;
        if ((bits & 0x1L) == 0) {
          newColumn(instance.newColumn());
          bits |= 0x1L;
        }
        if ((bits & 0x2L) == 0) {
          existingColumn(instance.existingColumn());
          bits |= 0x2L;
        }
      }
      if (object instanceof JoinAddition) {
        JoinAddition instance = (JoinAddition) object;
        if ((bits & 0x1L) == 0) {
          newColumn(instance.newColumn());
          bits |= 0x1L;
        }
        if ((bits & 0x2L) == 0) {
          existingColumn(instance.existingColumn());
          bits |= 0x2L;
        }
      }
    }

    /**
     * Initializes the value for the {@link JoinAdditionImpl#newColumn() newColumn} attribute.
     * @param newColumn The value for newColumn 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder newColumn(ColumnName newColumn) {
      this.newColumn = Objects.requireNonNull(newColumn, "newColumn");
      initBits &= ~INIT_BIT_NEW_COLUMN;
      return this;
    }

    /**
     * Initializes the value for the {@link JoinAdditionImpl#existingColumn() existingColumn} attribute.
     * @param existingColumn The value for existingColumn 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder existingColumn(ColumnName existingColumn) {
      this.existingColumn = Objects.requireNonNull(existingColumn, "existingColumn");
      initBits &= ~INIT_BIT_EXISTING_COLUMN;
      return this;
    }

    /**
     * Builds a new {@link ImmutableJoinAdditionImpl ImmutableJoinAdditionImpl}.
     * @return An immutable instance of JoinAdditionImpl
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableJoinAdditionImpl build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableJoinAdditionImpl.validate(new ImmutableJoinAdditionImpl(null, newColumn, existingColumn));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_NEW_COLUMN) != 0) attributes.add("newColumn");
      if ((initBits & INIT_BIT_EXISTING_COLUMN) != 0) attributes.add("existingColumn");
      return "Cannot build JoinAdditionImpl, some of required attributes are not set " + attributes;
    }
  }
}
