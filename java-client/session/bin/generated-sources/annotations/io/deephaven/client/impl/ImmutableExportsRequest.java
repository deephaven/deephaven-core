package io.deephaven.client.impl;

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
 * Immutable implementation of {@link ExportsRequest}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableExportsRequest.builder()}.
 */
@Generated(from = "ExportsRequest", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableExportsRequest extends ExportsRequest {
  private final ImmutableList<ExportRequest> requests;

  private ImmutableExportsRequest(ImmutableList<ExportRequest> requests) {
    this.requests = requests;
  }

  /**
   * @return The value of the {@code requests} attribute
   */
  @Override
  ImmutableList<ExportRequest> requests() {
    return requests;
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ExportsRequest#requests() requests}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableExportsRequest withRequests(ExportRequest... elements) {
    ImmutableList<ExportRequest> newValue = ImmutableList.copyOf(elements);
    return new ImmutableExportsRequest(newValue);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ExportsRequest#requests() requests}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of requests elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableExportsRequest withRequests(Iterable<? extends ExportRequest> elements) {
    if (this.requests == elements) return this;
    ImmutableList<ExportRequest> newValue = ImmutableList.copyOf(elements);
    return new ImmutableExportsRequest(newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableExportsRequest} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableExportsRequest
        && equalTo(0, (ImmutableExportsRequest) another);
  }

  private boolean equalTo(int synthetic, ImmutableExportsRequest another) {
    return requests.equals(another.requests);
  }

  /**
   * Computes a hash code from attributes: {@code requests}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + requests.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code ExportsRequest} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("ExportsRequest")
        .omitNullValues()
        .add("requests", requests)
        .toString();
  }

  /**
   * Creates an immutable copy of a {@link ExportsRequest} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ExportsRequest instance
   */
  public static ImmutableExportsRequest copyOf(ExportsRequest instance) {
    if (instance instanceof ImmutableExportsRequest) {
      return (ImmutableExportsRequest) instance;
    }
    return ImmutableExportsRequest.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableExportsRequest ImmutableExportsRequest}.
   * <pre>
   * ImmutableExportsRequest.builder()
   *    .addRequests|addAllRequests(io.deephaven.client.impl.ExportRequest) // {@link ExportsRequest#requests() requests} elements
   *    .build();
   * </pre>
   * @return A new ImmutableExportsRequest builder
   */
  public static ImmutableExportsRequest.Builder builder() {
    return new ImmutableExportsRequest.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableExportsRequest ImmutableExportsRequest}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ExportsRequest", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements ExportsRequest.Builder {
    private ImmutableList.Builder<ExportRequest> requests = ImmutableList.builder();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ExportsRequest} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(ExportsRequest instance) {
      Objects.requireNonNull(instance, "instance");
      addAllRequests(instance.requests());
      return this;
    }

    /**
     * Adds one element to {@link ExportsRequest#requests() requests} list.
     * @param element A requests element
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addRequests(ExportRequest element) {
      this.requests.add(element);
      return this;
    }

    /**
     * Adds elements to {@link ExportsRequest#requests() requests} list.
     * @param elements An array of requests elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addRequests(ExportRequest... elements) {
      this.requests.add(elements);
      return this;
    }


    /**
     * Sets or replaces all elements for {@link ExportsRequest#requests() requests} list.
     * @param elements An iterable of requests elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder requests(Iterable<? extends ExportRequest> elements) {
      this.requests = ImmutableList.builder();
      return addAllRequests(elements);
    }

    /**
     * Adds elements to {@link ExportsRequest#requests() requests} list.
     * @param elements An iterable of requests elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addAllRequests(Iterable<? extends ExportRequest> elements) {
      this.requests.addAll(elements);
      return this;
    }

    /**
     * Builds a new {@link ImmutableExportsRequest ImmutableExportsRequest}.
     * @return An immutable instance of ExportsRequest
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableExportsRequest build() {
      return new ImmutableExportsRequest(requests.build());
    }
  }
}
