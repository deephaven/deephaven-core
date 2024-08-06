package io.deephaven.ssl.config;

import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link TrustCustom}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableTrustCustom.builder()}.
 */
@Generated(from = "TrustCustom", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableTrustCustom extends TrustCustom {
  private final List<Certificate> certificates;

  private ImmutableTrustCustom(List<Certificate> certificates) {
    this.certificates = certificates;
  }

  /**
   * @return The value of the {@code certificates} attribute
   */
  @Override
  public List<Certificate> certificates() {
    return certificates;
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link TrustCustom#certificates() certificates}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableTrustCustom withCertificates(Certificate... elements) {
    List<Certificate> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return new ImmutableTrustCustom(newValue);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link TrustCustom#certificates() certificates}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of certificates elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableTrustCustom withCertificates(Iterable<? extends Certificate> elements) {
    if (this.certificates == elements) return this;
    List<Certificate> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return new ImmutableTrustCustom(newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableTrustCustom} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableTrustCustom
        && equalTo(0, (ImmutableTrustCustom) another);
  }

  private boolean equalTo(int synthetic, ImmutableTrustCustom another) {
    return certificates.equals(another.certificates);
  }

  /**
   * Computes a hash code from attributes: {@code certificates}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + certificates.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code TrustCustom} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "TrustCustom{"
        + "certificates=" + certificates
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link TrustCustom} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable TrustCustom instance
   */
  public static ImmutableTrustCustom copyOf(TrustCustom instance) {
    if (instance instanceof ImmutableTrustCustom) {
      return (ImmutableTrustCustom) instance;
    }
    return ImmutableTrustCustom.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableTrustCustom ImmutableTrustCustom}.
   * <pre>
   * ImmutableTrustCustom.builder()
   *    .addCertificates|addAllCertificates(java.security.cert.Certificate) // {@link TrustCustom#certificates() certificates} elements
   *    .build();
   * </pre>
   * @return A new ImmutableTrustCustom builder
   */
  public static ImmutableTrustCustom.Builder builder() {
    return new ImmutableTrustCustom.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableTrustCustom ImmutableTrustCustom}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "TrustCustom", generator = "Immutables")
  public static final class Builder implements TrustCustom.Builder {
    private List<Certificate> certificates = new ArrayList<Certificate>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code TrustCustom} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(TrustCustom instance) {
      Objects.requireNonNull(instance, "instance");
      addAllCertificates(instance.certificates());
      return this;
    }

    /**
     * Adds one element to {@link TrustCustom#certificates() certificates} list.
     * @param element A certificates element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addCertificates(Certificate element) {
      this.certificates.add(Objects.requireNonNull(element, "certificates element"));
      return this;
    }

    /**
     * Adds elements to {@link TrustCustom#certificates() certificates} list.
     * @param elements An array of certificates elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addCertificates(Certificate... elements) {
      for (Certificate element : elements) {
        this.certificates.add(Objects.requireNonNull(element, "certificates element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link TrustCustom#certificates() certificates} list.
     * @param elements An iterable of certificates elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder certificates(Iterable<? extends Certificate> elements) {
      this.certificates.clear();
      return addAllCertificates(elements);
    }

    /**
     * Adds elements to {@link TrustCustom#certificates() certificates} list.
     * @param elements An iterable of certificates elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllCertificates(Iterable<? extends Certificate> elements) {
      for (Certificate element : elements) {
        this.certificates.add(Objects.requireNonNull(element, "certificates element"));
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableTrustCustom ImmutableTrustCustom}.
     * @return An immutable instance of TrustCustom
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableTrustCustom build() {
      return new ImmutableTrustCustom(createUnmodifiableList(true, certificates));
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
