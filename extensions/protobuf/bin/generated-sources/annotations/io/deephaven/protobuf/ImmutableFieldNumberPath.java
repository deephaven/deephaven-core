package io.deephaven.protobuf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link FieldNumberPath}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableFieldNumberPath.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableFieldNumberPath.of()}.
 */
@Generated(from = "FieldNumberPath", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableFieldNumberPath extends FieldNumberPath {
  private final int[] path;

  private ImmutableFieldNumberPath(int[] path) {
    this.path = path.clone();
  }

  private ImmutableFieldNumberPath(ImmutableFieldNumberPath original, int[] path) {
    this.path = path;
  }

  /**
   * @return A cloned {@code path} array
   */
  @Override
  public int[] path() {
    return path.clone();
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link FieldNumberPath#path() path}.
   * The array is cloned before being saved as attribute values.
   * @param elements The non-null elements for path
   * @return A modified copy of {@code this} object
   */
  public final ImmutableFieldNumberPath withPath(int... elements) {
    int[] newValue = elements.clone();
    return new ImmutableFieldNumberPath(this, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableFieldNumberPath} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableFieldNumberPath
        && equalTo(0, (ImmutableFieldNumberPath) another);
  }

  private boolean equalTo(int synthetic, ImmutableFieldNumberPath another) {
    return Arrays.equals(path, another.path);
  }

  /**
   * Computes a hash code from attributes: {@code path}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Arrays.hashCode(path);
    return h;
  }

  /**
   * Prints the immutable value {@code FieldNumberPath} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "FieldNumberPath{"
        + "path=" + Arrays.toString(path)
        + "}";
  }

  /**
   * Construct a new immutable {@code FieldNumberPath} instance.
   * @param path The value for the {@code path} attribute
   * @return An immutable FieldNumberPath instance
   */
  public static ImmutableFieldNumberPath of(int[] path) {
    return new ImmutableFieldNumberPath(path);
  }

  /**
   * Creates an immutable copy of a {@link FieldNumberPath} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable FieldNumberPath instance
   */
  public static ImmutableFieldNumberPath copyOf(FieldNumberPath instance) {
    if (instance instanceof ImmutableFieldNumberPath) {
      return (ImmutableFieldNumberPath) instance;
    }
    return ImmutableFieldNumberPath.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableFieldNumberPath ImmutableFieldNumberPath}.
   * <pre>
   * ImmutableFieldNumberPath.builder()
   *    .path(int) // required {@link FieldNumberPath#path() path}
   *    .build();
   * </pre>
   * @return A new ImmutableFieldNumberPath builder
   */
  public static ImmutableFieldNumberPath.Builder builder() {
    return new ImmutableFieldNumberPath.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableFieldNumberPath ImmutableFieldNumberPath}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "FieldNumberPath", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_PATH = 0x1L;
    private long initBits = 0x1L;

    private int[] path;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code FieldNumberPath} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(FieldNumberPath instance) {
      Objects.requireNonNull(instance, "instance");
      path(instance.path());
      return this;
    }

    /**
     * Initializes the value for the {@link FieldNumberPath#path() path} attribute.
     * @param path The elements for path
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder path(int... path) {
      this.path = path.clone();
      initBits &= ~INIT_BIT_PATH;
      return this;
    }

    /**
     * Builds a new {@link ImmutableFieldNumberPath ImmutableFieldNumberPath}.
     * @return An immutable instance of FieldNumberPath
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableFieldNumberPath build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableFieldNumberPath(null, path);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_PATH) != 0) attributes.add("path");
      return "Cannot build FieldNumberPath, some of required attributes are not set " + attributes;
    }
  }
}
