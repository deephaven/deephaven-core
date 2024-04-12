package io.deephaven.appmode;

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
 * Immutable implementation of {@link Application}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableApplication.builder()}.
 */
@Generated(from = "Application", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableApplication extends Application {
  private final String id;
  private final String name;
  private final Fields fields;

  private ImmutableApplication(String id, String name, Fields fields) {
    this.id = id;
    this.name = name;
    this.fields = fields;
  }

  /**
   * The application id, should be unique and unchanging.
   * @return the application id
   */
  @Override
  public String id() {
    return id;
  }

  /**
   * The application name.
   * @return the application name
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * The fields.
   * @return the fields
   */
  @Override
  public Fields fields() {
    return fields;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link Application#id() id} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for id
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableApplication withId(String value) {
    String newValue = Objects.requireNonNull(value, "id");
    if (this.id.equals(newValue)) return this;
    return new ImmutableApplication(newValue, this.name, this.fields);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link Application#name() name} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for name
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableApplication withName(String value) {
    String newValue = Objects.requireNonNull(value, "name");
    if (this.name.equals(newValue)) return this;
    return new ImmutableApplication(this.id, newValue, this.fields);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link Application#fields() fields} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for fields
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableApplication withFields(Fields value) {
    if (this.fields == value) return this;
    Fields newValue = Objects.requireNonNull(value, "fields");
    return new ImmutableApplication(this.id, this.name, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableApplication} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableApplication
        && equalTo(0, (ImmutableApplication) another);
  }

  private boolean equalTo(int synthetic, ImmutableApplication another) {
    return id.equals(another.id)
        && name.equals(another.name)
        && fields.equals(another.fields);
  }

  /**
   * Computes a hash code from attributes: {@code id}, {@code name}, {@code fields}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + id.hashCode();
    h += (h << 5) + name.hashCode();
    h += (h << 5) + fields.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code Application} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("Application")
        .omitNullValues()
        .add("id", id)
        .add("name", name)
        .add("fields", fields)
        .toString();
  }

  /**
   * Creates an immutable copy of a {@link Application} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable Application instance
   */
  public static ImmutableApplication copyOf(Application instance) {
    if (instance instanceof ImmutableApplication) {
      return (ImmutableApplication) instance;
    }
    return ImmutableApplication.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableApplication ImmutableApplication}.
   * <pre>
   * ImmutableApplication.builder()
   *    .id(String) // required {@link Application#id() id}
   *    .name(String) // required {@link Application#name() name}
   *    .fields(io.deephaven.appmode.Fields) // required {@link Application#fields() fields}
   *    .build();
   * </pre>
   * @return A new ImmutableApplication builder
   */
  public static ImmutableApplication.Builder builder() {
    return new ImmutableApplication.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableApplication ImmutableApplication}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "Application", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements Application.Builder {
    private static final long INIT_BIT_ID = 0x1L;
    private static final long INIT_BIT_NAME = 0x2L;
    private static final long INIT_BIT_FIELDS = 0x4L;
    private long initBits = 0x7L;

    private @Nullable String id;
    private @Nullable String name;
    private @Nullable Fields fields;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code Application} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(Application instance) {
      Objects.requireNonNull(instance, "instance");
      id(instance.id());
      name(instance.name());
      fields(instance.fields());
      return this;
    }

    /**
     * Initializes the value for the {@link Application#id() id} attribute.
     * @param id The value for id 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder id(String id) {
      this.id = Objects.requireNonNull(id, "id");
      initBits &= ~INIT_BIT_ID;
      return this;
    }

    /**
     * Initializes the value for the {@link Application#name() name} attribute.
     * @param name The value for name 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder name(String name) {
      this.name = Objects.requireNonNull(name, "name");
      initBits &= ~INIT_BIT_NAME;
      return this;
    }

    /**
     * Initializes the value for the {@link Application#fields() fields} attribute.
     * @param fields The value for fields 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder fields(Fields fields) {
      this.fields = Objects.requireNonNull(fields, "fields");
      initBits &= ~INIT_BIT_FIELDS;
      return this;
    }

    /**
     * Builds a new {@link ImmutableApplication ImmutableApplication}.
     * @return An immutable instance of Application
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableApplication build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableApplication(id, name, fields);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_ID) != 0) attributes.add("id");
      if ((initBits & INIT_BIT_NAME) != 0) attributes.add("name");
      if ((initBits & INIT_BIT_FIELDS) != 0) attributes.add("fields");
      return "Cannot build Application, some of required attributes are not set " + attributes;
    }
  }
}
