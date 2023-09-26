package io.deephaven.appmode;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.Map;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link Fields}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableFields.builder()}.
 */
@Generated(from = "Fields", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableFields extends Fields {
  private final ImmutableMap<String, Field<?>> fields;

  private ImmutableFields(ImmutableMap<String, Field<?>> fields) {
    this.fields = fields;
  }

  /**
   * @return The value of the {@code fields} attribute
   */
  @Override
  ImmutableMap<String, Field<?>> fields() {
    return fields;
  }

  /**
   * Copy the current immutable object by replacing the {@link Fields#fields() fields} map with the specified map.
   * Nulls are not permitted as keys or values.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param entries The entries to be added to the fields map
   * @return A modified copy of {@code this} object
   */
  public final ImmutableFields withFields(Map<String, ? extends Field<?>> entries) {
    if (this.fields == entries) return this;
    ImmutableMap<String, Field<?>> newValue = ImmutableMap.copyOf(entries);
    return validate(new ImmutableFields(newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableFields} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableFields
        && equalTo(0, (ImmutableFields) another);
  }

  private boolean equalTo(int synthetic, ImmutableFields another) {
    return fields.equals(another.fields);
  }

  /**
   * Computes a hash code from attributes: {@code fields}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + fields.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code Fields} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("Fields")
        .omitNullValues()
        .add("fields", fields)
        .toString();
  }

  private static ImmutableFields validate(ImmutableFields instance) {
    instance.checkKeys();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link Fields} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable Fields instance
   */
  public static ImmutableFields copyOf(Fields instance) {
    if (instance instanceof ImmutableFields) {
      return (ImmutableFields) instance;
    }
    return ImmutableFields.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableFields ImmutableFields}.
   * <pre>
   * ImmutableFields.builder()
   *    .putFields|putAllFields(String =&gt; io.deephaven.appmode.Field&amp;lt;?&amp;gt;) // {@link Fields#fields() fields} mappings
   *    .build();
   * </pre>
   * @return A new ImmutableFields builder
   */
  public static ImmutableFields.Builder builder() {
    return new ImmutableFields.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableFields ImmutableFields}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "Fields", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements Fields.Builder {
    private ImmutableMap.Builder<String, Field<?>> fields = ImmutableMap.builder();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code Fields} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(Fields instance) {
      Objects.requireNonNull(instance, "instance");
      putAllFields(instance.fields());
      return this;
    }

    /**
     * Put one entry to the {@link Fields#fields() fields} map.
     * @param key The key in the fields map
     * @param value The associated value in the fields map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder putFields(String key, Field<?> value) {
      this.fields.put(key, value);
      return this;
    }

    /**
     * Put one entry to the {@link Fields#fields() fields} map. Nulls are not permitted
     * @param entry The key and value entry
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder putFields(Map.Entry<String, ? extends Field<?>> entry) {
      this.fields.put(entry);
      return this;
    }

    /**
     * Sets or replaces all mappings from the specified map as entries for the {@link Fields#fields() fields} map. Nulls are not permitted
     * @param entries The entries that will be added to the fields map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder fields(Map<String, ? extends Field<?>> entries) {
      this.fields = ImmutableMap.builder();
      return putAllFields(entries);
    }

    /**
     * Put all mappings from the specified map as entries to {@link Fields#fields() fields} map. Nulls are not permitted
     * @param entries The entries that will be added to the fields map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder putAllFields(Map<String, ? extends Field<?>> entries) {
      this.fields.putAll(entries);
      return this;
    }

    /**
     * Builds a new {@link ImmutableFields ImmutableFields}.
     * @return An immutable instance of Fields
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableFields build() {
      return ImmutableFields.validate(new ImmutableFields(fields.build()));
    }
  }
}
