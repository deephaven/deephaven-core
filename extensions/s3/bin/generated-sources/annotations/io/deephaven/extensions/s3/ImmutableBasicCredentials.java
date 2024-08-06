package io.deephaven.extensions.s3;

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
 * Immutable implementation of {@link BasicCredentials}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableBasicCredentials.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableBasicCredentials.of()}.
 */
@Generated(from = "BasicCredentials", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
final class ImmutableBasicCredentials extends BasicCredentials {
  private final String accessKeyId;
  private final String secretAccessKey;

  private ImmutableBasicCredentials(String accessKeyId, String secretAccessKey) {
    this.accessKeyId = Objects.requireNonNull(accessKeyId, "accessKeyId");
    this.secretAccessKey = Objects.requireNonNull(secretAccessKey, "secretAccessKey");
  }

  private ImmutableBasicCredentials(ImmutableBasicCredentials original, String accessKeyId, String secretAccessKey) {
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
  }

  /**
   * @return The value of the {@code accessKeyId} attribute
   */
  @Override
  String accessKeyId() {
    return accessKeyId;
  }

  /**
   * @return The value of the {@code secretAccessKey} attribute
   */
  @Override
  String secretAccessKey() {
    return secretAccessKey;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link BasicCredentials#accessKeyId() accessKeyId} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for accessKeyId
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableBasicCredentials withAccessKeyId(String value) {
    String newValue = Objects.requireNonNull(value, "accessKeyId");
    if (this.accessKeyId.equals(newValue)) return this;
    return new ImmutableBasicCredentials(this, newValue, this.secretAccessKey);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link BasicCredentials#secretAccessKey() secretAccessKey} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for secretAccessKey
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableBasicCredentials withSecretAccessKey(String value) {
    String newValue = Objects.requireNonNull(value, "secretAccessKey");
    if (this.secretAccessKey.equals(newValue)) return this;
    return new ImmutableBasicCredentials(this, this.accessKeyId, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableBasicCredentials} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableBasicCredentials
        && equalTo(0, (ImmutableBasicCredentials) another);
  }

  private boolean equalTo(int synthetic, ImmutableBasicCredentials another) {
    return accessKeyId.equals(another.accessKeyId)
        && secretAccessKey.equals(another.secretAccessKey);
  }

  /**
   * Computes a hash code from attributes: {@code accessKeyId}, {@code secretAccessKey}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + accessKeyId.hashCode();
    h += (h << 5) + secretAccessKey.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code BasicCredentials} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("BasicCredentials")
        .omitNullValues()
        .add("accessKeyId", accessKeyId)
        .toString();
  }

  /**
   * Construct a new immutable {@code BasicCredentials} instance.
   * @param accessKeyId The value for the {@code accessKeyId} attribute
   * @param secretAccessKey The value for the {@code secretAccessKey} attribute
   * @return An immutable BasicCredentials instance
   */
  public static ImmutableBasicCredentials of(String accessKeyId, String secretAccessKey) {
    return new ImmutableBasicCredentials(accessKeyId, secretAccessKey);
  }

  /**
   * Creates an immutable copy of a {@link BasicCredentials} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable BasicCredentials instance
   */
  public static ImmutableBasicCredentials copyOf(BasicCredentials instance) {
    if (instance instanceof ImmutableBasicCredentials) {
      return (ImmutableBasicCredentials) instance;
    }
    return ImmutableBasicCredentials.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableBasicCredentials ImmutableBasicCredentials}.
   * <pre>
   * ImmutableBasicCredentials.builder()
   *    .accessKeyId(String) // required {@link BasicCredentials#accessKeyId() accessKeyId}
   *    .secretAccessKey(String) // required {@link BasicCredentials#secretAccessKey() secretAccessKey}
   *    .build();
   * </pre>
   * @return A new ImmutableBasicCredentials builder
   */
  public static ImmutableBasicCredentials.Builder builder() {
    return new ImmutableBasicCredentials.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableBasicCredentials ImmutableBasicCredentials}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "BasicCredentials", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_ACCESS_KEY_ID = 0x1L;
    private static final long INIT_BIT_SECRET_ACCESS_KEY = 0x2L;
    private long initBits = 0x3L;

    private @Nullable String accessKeyId;
    private @Nullable String secretAccessKey;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code BasicCredentials} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(BasicCredentials instance) {
      Objects.requireNonNull(instance, "instance");
      accessKeyId(instance.accessKeyId());
      secretAccessKey(instance.secretAccessKey());
      return this;
    }

    /**
     * Initializes the value for the {@link BasicCredentials#accessKeyId() accessKeyId} attribute.
     * @param accessKeyId The value for accessKeyId 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder accessKeyId(String accessKeyId) {
      this.accessKeyId = Objects.requireNonNull(accessKeyId, "accessKeyId");
      initBits &= ~INIT_BIT_ACCESS_KEY_ID;
      return this;
    }

    /**
     * Initializes the value for the {@link BasicCredentials#secretAccessKey() secretAccessKey} attribute.
     * @param secretAccessKey The value for secretAccessKey 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder secretAccessKey(String secretAccessKey) {
      this.secretAccessKey = Objects.requireNonNull(secretAccessKey, "secretAccessKey");
      initBits &= ~INIT_BIT_SECRET_ACCESS_KEY;
      return this;
    }

    /**
     * Builds a new {@link ImmutableBasicCredentials ImmutableBasicCredentials}.
     * @return An immutable instance of BasicCredentials
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableBasicCredentials build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableBasicCredentials(null, accessKeyId, secretAccessKey);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_ACCESS_KEY_ID) != 0) attributes.add("accessKeyId");
      if ((initBits & INIT_BIT_SECRET_ACCESS_KEY) != 0) attributes.add("secretAccessKey");
      return "Cannot build BasicCredentials, some of required attributes are not set " + attributes;
    }
  }
}
