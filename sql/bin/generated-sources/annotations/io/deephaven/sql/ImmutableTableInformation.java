package io.deephaven.sql;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.TableSpec;
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
 * Immutable implementation of {@link TableInformation}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableTableInformation.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableTableInformation.of()}.
 */
@Generated(from = "TableInformation", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableTableInformation extends TableInformation {
  private final ImmutableList<String> qualifiedName;
  private final TableHeader header;
  private final TableSpec spec;

  private ImmutableTableInformation(
      Iterable<String> qualifiedName,
      TableHeader header,
      TableSpec spec) {
    this.qualifiedName = ImmutableList.copyOf(qualifiedName);
    this.header = Objects.requireNonNull(header, "header");
    this.spec = Objects.requireNonNull(spec, "spec");
  }

  private ImmutableTableInformation(
      ImmutableTableInformation original,
      ImmutableList<String> qualifiedName,
      TableHeader header,
      TableSpec spec) {
    this.qualifiedName = qualifiedName;
    this.header = header;
    this.spec = spec;
  }

  /**
   * @return The value of the {@code qualifiedName} attribute
   */
  @Override
  public ImmutableList<String> qualifiedName() {
    return qualifiedName;
  }

  /**
   * @return The value of the {@code header} attribute
   */
  @Override
  public TableHeader header() {
    return header;
  }

  /**
   * @return The value of the {@code spec} attribute
   */
  @Override
  public TableSpec spec() {
    return spec;
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link TableInformation#qualifiedName() qualifiedName}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableTableInformation withQualifiedName(String... elements) {
    ImmutableList<String> newValue = ImmutableList.copyOf(elements);
    return new ImmutableTableInformation(this, newValue, this.header, this.spec);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link TableInformation#qualifiedName() qualifiedName}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of qualifiedName elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableTableInformation withQualifiedName(Iterable<String> elements) {
    if (this.qualifiedName == elements) return this;
    ImmutableList<String> newValue = ImmutableList.copyOf(elements);
    return new ImmutableTableInformation(this, newValue, this.header, this.spec);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TableInformation#header() header} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for header
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTableInformation withHeader(TableHeader value) {
    if (this.header == value) return this;
    TableHeader newValue = Objects.requireNonNull(value, "header");
    return new ImmutableTableInformation(this, this.qualifiedName, newValue, this.spec);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TableInformation#spec() spec} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for spec
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTableInformation withSpec(TableSpec value) {
    if (this.spec == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "spec");
    return new ImmutableTableInformation(this, this.qualifiedName, this.header, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableTableInformation} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableTableInformation
        && equalTo(0, (ImmutableTableInformation) another);
  }

  private boolean equalTo(int synthetic, ImmutableTableInformation another) {
    return qualifiedName.equals(another.qualifiedName)
        && header.equals(another.header)
        && spec.equals(another.spec);
  }

  /**
   * Computes a hash code from attributes: {@code qualifiedName}, {@code header}, {@code spec}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + qualifiedName.hashCode();
    h += (h << 5) + header.hashCode();
    h += (h << 5) + spec.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code TableInformation} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("TableInformation")
        .omitNullValues()
        .add("qualifiedName", qualifiedName)
        .add("header", header)
        .add("spec", spec)
        .toString();
  }

  /**
   * Construct a new immutable {@code TableInformation} instance.
   * @param qualifiedName The value for the {@code qualifiedName} attribute
   * @param header The value for the {@code header} attribute
   * @param spec The value for the {@code spec} attribute
   * @return An immutable TableInformation instance
   */
  public static ImmutableTableInformation of(List<String> qualifiedName, TableHeader header, TableSpec spec) {
    return of((Iterable<String>) qualifiedName, header, spec);
  }

  /**
   * Construct a new immutable {@code TableInformation} instance.
   * @param qualifiedName The value for the {@code qualifiedName} attribute
   * @param header The value for the {@code header} attribute
   * @param spec The value for the {@code spec} attribute
   * @return An immutable TableInformation instance
   */
  public static ImmutableTableInformation of(Iterable<String> qualifiedName, TableHeader header, TableSpec spec) {
    return new ImmutableTableInformation(qualifiedName, header, spec);
  }

  /**
   * Creates an immutable copy of a {@link TableInformation} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable TableInformation instance
   */
  public static ImmutableTableInformation copyOf(TableInformation instance) {
    if (instance instanceof ImmutableTableInformation) {
      return (ImmutableTableInformation) instance;
    }
    return ImmutableTableInformation.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableTableInformation ImmutableTableInformation}.
   * <pre>
   * ImmutableTableInformation.builder()
   *    .addQualifiedName|addAllQualifiedName(String) // {@link TableInformation#qualifiedName() qualifiedName} elements
   *    .header(io.deephaven.qst.table.TableHeader) // required {@link TableInformation#header() header}
   *    .spec(io.deephaven.qst.table.TableSpec) // required {@link TableInformation#spec() spec}
   *    .build();
   * </pre>
   * @return A new ImmutableTableInformation builder
   */
  public static ImmutableTableInformation.Builder builder() {
    return new ImmutableTableInformation.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableTableInformation ImmutableTableInformation}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "TableInformation", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_HEADER = 0x1L;
    private static final long INIT_BIT_SPEC = 0x2L;
    private long initBits = 0x3L;

    private ImmutableList.Builder<String> qualifiedName = ImmutableList.builder();
    private @Nullable TableHeader header;
    private @Nullable TableSpec spec;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code TableInformation} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(TableInformation instance) {
      Objects.requireNonNull(instance, "instance");
      addAllQualifiedName(instance.qualifiedName());
      header(instance.header());
      spec(instance.spec());
      return this;
    }

    /**
     * Adds one element to {@link TableInformation#qualifiedName() qualifiedName} list.
     * @param element A qualifiedName element
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addQualifiedName(String element) {
      this.qualifiedName.add(element);
      return this;
    }

    /**
     * Adds elements to {@link TableInformation#qualifiedName() qualifiedName} list.
     * @param elements An array of qualifiedName elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addQualifiedName(String... elements) {
      this.qualifiedName.add(elements);
      return this;
    }


    /**
     * Sets or replaces all elements for {@link TableInformation#qualifiedName() qualifiedName} list.
     * @param elements An iterable of qualifiedName elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder qualifiedName(Iterable<String> elements) {
      this.qualifiedName = ImmutableList.builder();
      return addAllQualifiedName(elements);
    }

    /**
     * Adds elements to {@link TableInformation#qualifiedName() qualifiedName} list.
     * @param elements An iterable of qualifiedName elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addAllQualifiedName(Iterable<String> elements) {
      this.qualifiedName.addAll(elements);
      return this;
    }

    /**
     * Initializes the value for the {@link TableInformation#header() header} attribute.
     * @param header The value for header 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder header(TableHeader header) {
      this.header = Objects.requireNonNull(header, "header");
      initBits &= ~INIT_BIT_HEADER;
      return this;
    }

    /**
     * Initializes the value for the {@link TableInformation#spec() spec} attribute.
     * @param spec The value for spec 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder spec(TableSpec spec) {
      this.spec = Objects.requireNonNull(spec, "spec");
      initBits &= ~INIT_BIT_SPEC;
      return this;
    }

    /**
     * Builds a new {@link ImmutableTableInformation ImmutableTableInformation}.
     * @return An immutable instance of TableInformation
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableTableInformation build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableTableInformation(null, qualifiedName.build(), header, spec);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_HEADER) != 0) attributes.add("header");
      if ((initBits & INIT_BIT_SPEC) != 0) attributes.add("spec");
      return "Cannot build TableInformation, some of required attributes are not set " + attributes;
    }
  }
}
