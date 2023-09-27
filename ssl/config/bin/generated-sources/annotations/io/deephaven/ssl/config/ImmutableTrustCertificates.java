package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link TrustCertificates}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableTrustCertificates.builder()}.
 */
@Generated(from = "TrustCertificates", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableTrustCertificates extends TrustCertificates {
  private final List<String> path;

  private ImmutableTrustCertificates(List<String> path) {
    this.path = path;
  }

  /**
   * The certificate paths.
   */
  @JsonProperty("path")
  @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
  @Override
  public List<String> path() {
    return path;
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link TrustCertificates#path() path}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableTrustCertificates withPath(String... elements) {
    List<String> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableTrustCertificates(newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link TrustCertificates#path() path}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of path elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableTrustCertificates withPath(Iterable<String> elements) {
    if (this.path == elements) return this;
    List<String> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableTrustCertificates(newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableTrustCertificates} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableTrustCertificates
        && equalTo(0, (ImmutableTrustCertificates) another);
  }

  private boolean equalTo(int synthetic, ImmutableTrustCertificates another) {
    return path.equals(another.path);
  }

  /**
   * Computes a hash code from attributes: {@code path}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + path.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code TrustCertificates} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "TrustCertificates{"
        + "path=" + path
        + "}";
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "TrustCertificates", generator = "Immutables")
  @Deprecated
  @JsonDeserialize
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends TrustCertificates {
    List<String> path = Collections.emptyList();
    @JsonProperty("path")
    @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
    public void setPath(List<String> path) {
      this.path = path;
    }
    @Override
    public List<String> path() { throw new UnsupportedOperationException(); }
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static ImmutableTrustCertificates fromJson(Json json) {
    ImmutableTrustCertificates.Builder builder = ImmutableTrustCertificates.builder();
    if (json.path != null) {
      builder.addAllPath(json.path);
    }
    return builder.build();
  }

  private static ImmutableTrustCertificates validate(ImmutableTrustCertificates instance) {
    instance.checkPath();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link TrustCertificates} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable TrustCertificates instance
   */
  public static ImmutableTrustCertificates copyOf(TrustCertificates instance) {
    if (instance instanceof ImmutableTrustCertificates) {
      return (ImmutableTrustCertificates) instance;
    }
    return ImmutableTrustCertificates.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableTrustCertificates ImmutableTrustCertificates}.
   * <pre>
   * ImmutableTrustCertificates.builder()
   *    .addPath|addAllPath(String) // {@link TrustCertificates#path() path} elements
   *    .build();
   * </pre>
   * @return A new ImmutableTrustCertificates builder
   */
  public static ImmutableTrustCertificates.Builder builder() {
    return new ImmutableTrustCertificates.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableTrustCertificates ImmutableTrustCertificates}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "TrustCertificates", generator = "Immutables")
  public static final class Builder {
    private List<String> path = new ArrayList<String>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code TrustCertificates} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(TrustCertificates instance) {
      Objects.requireNonNull(instance, "instance");
      addAllPath(instance.path());
      return this;
    }

    /**
     * Adds one element to {@link TrustCertificates#path() path} list.
     * @param element A path element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addPath(String element) {
      this.path.add(Objects.requireNonNull(element, "path element"));
      return this;
    }

    /**
     * Adds elements to {@link TrustCertificates#path() path} list.
     * @param elements An array of path elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addPath(String... elements) {
      for (String element : elements) {
        this.path.add(Objects.requireNonNull(element, "path element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link TrustCertificates#path() path} list.
     * @param elements An iterable of path elements
     * @return {@code this} builder for use in a chained invocation
     */
    @JsonProperty("path")
    @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
    public final Builder path(Iterable<String> elements) {
      this.path.clear();
      return addAllPath(elements);
    }

    /**
     * Adds elements to {@link TrustCertificates#path() path} list.
     * @param elements An iterable of path elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllPath(Iterable<String> elements) {
      for (String element : elements) {
        this.path.add(Objects.requireNonNull(element, "path element"));
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableTrustCertificates ImmutableTrustCertificates}.
     * @return An immutable instance of TrustCertificates
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableTrustCertificates build() {
      return ImmutableTrustCertificates.validate(new ImmutableTrustCertificates(createUnmodifiableList(true, path)));
    }
  }

  private static <T> List<T> createSafeList(Iterable<? extends T> iterable, boolean checkNulls, boolean skipNulls) {
    ArrayList<T> list;
    if (iterable instanceof Collection<?>) {
      int size = ((Collection<?>) iterable).size();
      if (size == 0) return Collections.emptyList();
      list = new ArrayList<>();
    } else {
      list = new ArrayList<>();
    }
    for (T element : iterable) {
      if (skipNulls && element == null) continue;
      if (checkNulls) Objects.requireNonNull(element, "element");
      list.add(element);
    }
    return list;
  }

  private static <T> List<T> createUnmodifiableList(boolean clone, List<T> list) {
    switch(list.size()) {
    case 0: return Collections.emptyList();
    case 1: return Collections.singletonList(list.get(0));
    default:
      if (clone) {
        return Collections.unmodifiableList(new ArrayList<>(list));
      } else {
        if (list instanceof ArrayList<?>) {
          ((ArrayList<?>) list).trimToSize();
        }
        return Collections.unmodifiableList(list);
      }
    }
  }
}
