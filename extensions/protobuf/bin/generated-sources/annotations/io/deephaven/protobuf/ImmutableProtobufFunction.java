package io.deephaven.protobuf;

import com.google.protobuf.Message;
import io.deephaven.functions.TypedFunction;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ProtobufFunction}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableProtobufFunction.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableProtobufFunction.of()}.
 */
@Generated(from = "ProtobufFunction", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableProtobufFunction extends ProtobufFunction {
  private final FieldPath path;
  private final TypedFunction<Message> function;

  private ImmutableProtobufFunction(
      FieldPath path,
      TypedFunction<Message> function) {
    this.path = Objects.requireNonNull(path, "path");
    this.function = Objects.requireNonNull(function, "function");
  }

  private ImmutableProtobufFunction(
      ImmutableProtobufFunction original,
      FieldPath path,
      TypedFunction<Message> function) {
    this.path = path;
    this.function = function;
  }

  /**
   * The path that {@link #function()} uses to produce its result.
   * @return the path
   */
  @Override
  public FieldPath path() {
    return path;
  }

  /**
   * The function to extract a result from a {@link Message}.
   * @return the function
   */
  @Override
  public TypedFunction<Message> function() {
    return function;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ProtobufFunction#path() path} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for path
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableProtobufFunction withPath(FieldPath value) {
    if (this.path == value) return this;
    FieldPath newValue = Objects.requireNonNull(value, "path");
    return new ImmutableProtobufFunction(this, newValue, this.function);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ProtobufFunction#function() function} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for function
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableProtobufFunction withFunction(TypedFunction<Message> value) {
    if (this.function == value) return this;
    TypedFunction<Message> newValue = Objects.requireNonNull(value, "function");
    return new ImmutableProtobufFunction(this, this.path, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableProtobufFunction} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableProtobufFunction
        && equalTo(0, (ImmutableProtobufFunction) another);
  }

  private boolean equalTo(int synthetic, ImmutableProtobufFunction another) {
    return path.equals(another.path)
        && function.equals(another.function);
  }

  /**
   * Computes a hash code from attributes: {@code path}, {@code function}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + path.hashCode();
    h += (h << 5) + function.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code ProtobufFunction} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ProtobufFunction{"
        + "path=" + path
        + ", function=" + function
        + "}";
  }

  /**
   * Construct a new immutable {@code ProtobufFunction} instance.
   * @param path The value for the {@code path} attribute
   * @param function The value for the {@code function} attribute
   * @return An immutable ProtobufFunction instance
   */
  public static ImmutableProtobufFunction of(FieldPath path, TypedFunction<Message> function) {
    return new ImmutableProtobufFunction(path, function);
  }

  /**
   * Creates an immutable copy of a {@link ProtobufFunction} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ProtobufFunction instance
   */
  public static ImmutableProtobufFunction copyOf(ProtobufFunction instance) {
    if (instance instanceof ImmutableProtobufFunction) {
      return (ImmutableProtobufFunction) instance;
    }
    return ImmutableProtobufFunction.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableProtobufFunction ImmutableProtobufFunction}.
   * <pre>
   * ImmutableProtobufFunction.builder()
   *    .path(io.deephaven.protobuf.FieldPath) // required {@link ProtobufFunction#path() path}
   *    .function(io.deephaven.functions.TypedFunction&amp;lt;com.google.protobuf.Message&amp;gt;) // required {@link ProtobufFunction#function() function}
   *    .build();
   * </pre>
   * @return A new ImmutableProtobufFunction builder
   */
  public static ImmutableProtobufFunction.Builder builder() {
    return new ImmutableProtobufFunction.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableProtobufFunction ImmutableProtobufFunction}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ProtobufFunction", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_PATH = 0x1L;
    private static final long INIT_BIT_FUNCTION = 0x2L;
    private long initBits = 0x3L;

    private FieldPath path;
    private TypedFunction<Message> function;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ProtobufFunction} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(ProtobufFunction instance) {
      Objects.requireNonNull(instance, "instance");
      path(instance.path());
      function(instance.function());
      return this;
    }

    /**
     * Initializes the value for the {@link ProtobufFunction#path() path} attribute.
     * @param path The value for path 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder path(FieldPath path) {
      this.path = Objects.requireNonNull(path, "path");
      initBits &= ~INIT_BIT_PATH;
      return this;
    }

    /**
     * Initializes the value for the {@link ProtobufFunction#function() function} attribute.
     * @param function The value for function 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder function(TypedFunction<Message> function) {
      this.function = Objects.requireNonNull(function, "function");
      initBits &= ~INIT_BIT_FUNCTION;
      return this;
    }

    /**
     * Builds a new {@link ImmutableProtobufFunction ImmutableProtobufFunction}.
     * @return An immutable instance of ProtobufFunction
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableProtobufFunction build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableProtobufFunction(null, path, function);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_PATH) != 0) attributes.add("path");
      if ((initBits & INIT_BIT_FUNCTION) != 0) attributes.add("function");
      return "Cannot build ProtobufFunction, some of required attributes are not set " + attributes;
    }
  }
}
