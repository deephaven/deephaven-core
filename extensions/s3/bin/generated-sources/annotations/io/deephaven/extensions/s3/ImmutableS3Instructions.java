package io.deephaven.extensions.s3;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link S3Instructions}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableS3Instructions.builder()}.
 */
@Generated(from = "S3Instructions", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableS3Instructions extends S3Instructions {
  private final @Nullable String regionName;
  private final int maxConcurrentRequests;
  private final int readAheadCount;
  private final int fragmentSize;
  private final Duration connectionTimeout;
  private final Duration readTimeout;
  private final Credentials credentials;
  private final @Nullable URI endpointOverride;

  private ImmutableS3Instructions(ImmutableS3Instructions.Builder builder) {
    this.regionName = builder.regionName;
    this.endpointOverride = builder.endpointOverride;
    if (builder.maxConcurrentRequestsIsSet()) {
      initShim.maxConcurrentRequests(builder.maxConcurrentRequests);
    }
    if (builder.readAheadCountIsSet()) {
      initShim.readAheadCount(builder.readAheadCount);
    }
    if (builder.fragmentSizeIsSet()) {
      initShim.fragmentSize(builder.fragmentSize);
    }
    if (builder.connectionTimeout != null) {
      initShim.connectionTimeout(builder.connectionTimeout);
    }
    if (builder.readTimeout != null) {
      initShim.readTimeout(builder.readTimeout);
    }
    if (builder.credentials != null) {
      initShim.credentials(builder.credentials);
    }
    this.maxConcurrentRequests = initShim.maxConcurrentRequests();
    this.readAheadCount = initShim.readAheadCount();
    this.fragmentSize = initShim.fragmentSize();
    this.connectionTimeout = initShim.connectionTimeout();
    this.readTimeout = initShim.readTimeout();
    this.credentials = initShim.credentials();
    this.initShim = null;
  }

  private ImmutableS3Instructions(
      @Nullable String regionName,
      int maxConcurrentRequests,
      int readAheadCount,
      int fragmentSize,
      Duration connectionTimeout,
      Duration readTimeout,
      Credentials credentials,
      @Nullable URI endpointOverride) {
    this.regionName = regionName;
    this.maxConcurrentRequests = maxConcurrentRequests;
    this.readAheadCount = readAheadCount;
    this.fragmentSize = fragmentSize;
    this.connectionTimeout = connectionTimeout;
    this.readTimeout = readTimeout;
    this.credentials = credentials;
    this.endpointOverride = endpointOverride;
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  @SuppressWarnings("Immutable")
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "S3Instructions", generator = "Immutables")
  private final class InitShim {
    private byte maxConcurrentRequestsBuildStage = STAGE_UNINITIALIZED;
    private int maxConcurrentRequests;

    int maxConcurrentRequests() {
      if (maxConcurrentRequestsBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (maxConcurrentRequestsBuildStage == STAGE_UNINITIALIZED) {
        maxConcurrentRequestsBuildStage = STAGE_INITIALIZING;
        this.maxConcurrentRequests = ImmutableS3Instructions.super.maxConcurrentRequests();
        maxConcurrentRequestsBuildStage = STAGE_INITIALIZED;
      }
      return this.maxConcurrentRequests;
    }

    void maxConcurrentRequests(int maxConcurrentRequests) {
      this.maxConcurrentRequests = maxConcurrentRequests;
      maxConcurrentRequestsBuildStage = STAGE_INITIALIZED;
    }

    private byte readAheadCountBuildStage = STAGE_UNINITIALIZED;
    private int readAheadCount;

    int readAheadCount() {
      if (readAheadCountBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (readAheadCountBuildStage == STAGE_UNINITIALIZED) {
        readAheadCountBuildStage = STAGE_INITIALIZING;
        this.readAheadCount = ImmutableS3Instructions.super.readAheadCount();
        readAheadCountBuildStage = STAGE_INITIALIZED;
      }
      return this.readAheadCount;
    }

    void readAheadCount(int readAheadCount) {
      this.readAheadCount = readAheadCount;
      readAheadCountBuildStage = STAGE_INITIALIZED;
    }

    private byte fragmentSizeBuildStage = STAGE_UNINITIALIZED;
    private int fragmentSize;

    int fragmentSize() {
      if (fragmentSizeBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (fragmentSizeBuildStage == STAGE_UNINITIALIZED) {
        fragmentSizeBuildStage = STAGE_INITIALIZING;
        this.fragmentSize = ImmutableS3Instructions.super.fragmentSize();
        fragmentSizeBuildStage = STAGE_INITIALIZED;
      }
      return this.fragmentSize;
    }

    void fragmentSize(int fragmentSize) {
      this.fragmentSize = fragmentSize;
      fragmentSizeBuildStage = STAGE_INITIALIZED;
    }

    private byte connectionTimeoutBuildStage = STAGE_UNINITIALIZED;
    private Duration connectionTimeout;

    Duration connectionTimeout() {
      if (connectionTimeoutBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (connectionTimeoutBuildStage == STAGE_UNINITIALIZED) {
        connectionTimeoutBuildStage = STAGE_INITIALIZING;
        this.connectionTimeout = Objects.requireNonNull(ImmutableS3Instructions.super.connectionTimeout(), "connectionTimeout");
        connectionTimeoutBuildStage = STAGE_INITIALIZED;
      }
      return this.connectionTimeout;
    }

    void connectionTimeout(Duration connectionTimeout) {
      this.connectionTimeout = connectionTimeout;
      connectionTimeoutBuildStage = STAGE_INITIALIZED;
    }

    private byte readTimeoutBuildStage = STAGE_UNINITIALIZED;
    private Duration readTimeout;

    Duration readTimeout() {
      if (readTimeoutBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (readTimeoutBuildStage == STAGE_UNINITIALIZED) {
        readTimeoutBuildStage = STAGE_INITIALIZING;
        this.readTimeout = Objects.requireNonNull(ImmutableS3Instructions.super.readTimeout(), "readTimeout");
        readTimeoutBuildStage = STAGE_INITIALIZED;
      }
      return this.readTimeout;
    }

    void readTimeout(Duration readTimeout) {
      this.readTimeout = readTimeout;
      readTimeoutBuildStage = STAGE_INITIALIZED;
    }

    private byte credentialsBuildStage = STAGE_UNINITIALIZED;
    private Credentials credentials;

    Credentials credentials() {
      if (credentialsBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (credentialsBuildStage == STAGE_UNINITIALIZED) {
        credentialsBuildStage = STAGE_INITIALIZING;
        this.credentials = Objects.requireNonNull(ImmutableS3Instructions.super.credentials(), "credentials");
        credentialsBuildStage = STAGE_INITIALIZED;
      }
      return this.credentials;
    }

    void credentials(Credentials credentials) {
      this.credentials = credentials;
      credentialsBuildStage = STAGE_INITIALIZED;
    }

    private String formatInitCycleMessage() {
      List<String> attributes = new ArrayList<>();
      if (maxConcurrentRequestsBuildStage == STAGE_INITIALIZING) attributes.add("maxConcurrentRequests");
      if (readAheadCountBuildStage == STAGE_INITIALIZING) attributes.add("readAheadCount");
      if (fragmentSizeBuildStage == STAGE_INITIALIZING) attributes.add("fragmentSize");
      if (connectionTimeoutBuildStage == STAGE_INITIALIZING) attributes.add("connectionTimeout");
      if (readTimeoutBuildStage == STAGE_INITIALIZING) attributes.add("readTimeout");
      if (credentialsBuildStage == STAGE_INITIALIZING) attributes.add("credentials");
      return "Cannot build S3Instructions, attribute initializers form cycle " + attributes;
    }
  }

  /**
   * The region name to use when reading or writing to S3. If not provided, the region name is picked by the AWS SDK
   * from 'aws.region' system property, "AWS_REGION" environment variable, the {user.home}/.aws/credentials or
   * {user.home}/.aws/config files, or from EC2 metadata service, if running in EC2.
   */
  @Override
  public Optional<String> regionName() {
    return Optional.ofNullable(regionName);
  }

  /**
   * The maximum number of concurrent requests to make to S3, defaults to {@value #DEFAULT_MAX_CONCURRENT_REQUESTS}.
   */
  @Override
  public int maxConcurrentRequests() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.maxConcurrentRequests()
        : this.maxConcurrentRequests;
  }

  /**
   * The number of fragments to send asynchronous read requests for while reading the current fragment. Defaults to
   * {@value #DEFAULT_READ_AHEAD_COUNT}, which means by default, we will fetch {@value #DEFAULT_READ_AHEAD_COUNT}
   * fragments in advance when reading current fragment.
   */
  @Override
  public int readAheadCount() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.readAheadCount()
        : this.readAheadCount;
  }

  /**
   * The maximum byte size of each fragment to read from S3, defaults to {@value DEFAULT_FRAGMENT_SIZE}, must be
   * larger than {@value MIN_FRAGMENT_SIZE}. If there are fewer bytes remaining in the file, the fetched fragment can
   * be smaller.
   */
  @Override
  public int fragmentSize() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.fragmentSize()
        : this.fragmentSize;
  }

  /**
   * The amount of time to wait when initially establishing a connection before giving up and timing out, defaults to
   * 2 seconds.
   */
  @Override
  public Duration connectionTimeout() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.connectionTimeout()
        : this.connectionTimeout;
  }

  /**
   * The amount of time to wait when reading a fragment before giving up and timing out, defaults to 2 seconds. The
   * implementation may choose to internally retry the request multiple times, so long as the total time does not
   * exceed this timeout.
   */
  @Override
  public Duration readTimeout() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.readTimeout()
        : this.readTimeout;
  }

  /**
   * The credentials to use when reading or writing to S3. By default, uses {@link Credentials#defaultCredentials()}.
   */
  @Override
  public Credentials credentials() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.credentials()
        : this.credentials;
  }

  /**
   * The endpoint to connect to. Callers connecting to AWS do not typically need to set this; it is most useful when
   * connecting to non-AWS, S3-compatible APIs.
   * @see <a href="https://docs.aws.amazon.com/general/latest/gr/s3.html">Amazon Simple Storage Service endpoints</a>
   */
  @Override
  public Optional<URI> endpointOverride() {
    return Optional.ofNullable(endpointOverride);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link S3Instructions#regionName() regionName} attribute.
   * @param value The value for regionName
   * @return A modified copy of {@code this} object
   */
  public final ImmutableS3Instructions withRegionName(String value) {
    @Nullable String newValue = Objects.requireNonNull(value, "regionName");
    if (Objects.equals(this.regionName, newValue)) return this;
    return validate(new ImmutableS3Instructions(
        newValue,
        this.maxConcurrentRequests,
        this.readAheadCount,
        this.fragmentSize,
        this.connectionTimeout,
        this.readTimeout,
        this.credentials,
        this.endpointOverride));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link S3Instructions#regionName() regionName} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for regionName
   * @return A modified copy of {@code this} object
   */
  public final ImmutableS3Instructions withRegionName(Optional<String> optional) {
    @Nullable String value = optional.orElse(null);
    if (Objects.equals(this.regionName, value)) return this;
    return validate(new ImmutableS3Instructions(
        value,
        this.maxConcurrentRequests,
        this.readAheadCount,
        this.fragmentSize,
        this.connectionTimeout,
        this.readTimeout,
        this.credentials,
        this.endpointOverride));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link S3Instructions#maxConcurrentRequests() maxConcurrentRequests} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for maxConcurrentRequests
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableS3Instructions withMaxConcurrentRequests(int value) {
    if (this.maxConcurrentRequests == value) return this;
    return validate(new ImmutableS3Instructions(
        this.regionName,
        value,
        this.readAheadCount,
        this.fragmentSize,
        this.connectionTimeout,
        this.readTimeout,
        this.credentials,
        this.endpointOverride));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link S3Instructions#readAheadCount() readAheadCount} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for readAheadCount
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableS3Instructions withReadAheadCount(int value) {
    if (this.readAheadCount == value) return this;
    return validate(new ImmutableS3Instructions(
        this.regionName,
        this.maxConcurrentRequests,
        value,
        this.fragmentSize,
        this.connectionTimeout,
        this.readTimeout,
        this.credentials,
        this.endpointOverride));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link S3Instructions#fragmentSize() fragmentSize} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for fragmentSize
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableS3Instructions withFragmentSize(int value) {
    if (this.fragmentSize == value) return this;
    return validate(new ImmutableS3Instructions(
        this.regionName,
        this.maxConcurrentRequests,
        this.readAheadCount,
        value,
        this.connectionTimeout,
        this.readTimeout,
        this.credentials,
        this.endpointOverride));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link S3Instructions#connectionTimeout() connectionTimeout} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for connectionTimeout
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableS3Instructions withConnectionTimeout(Duration value) {
    if (this.connectionTimeout == value) return this;
    Duration newValue = Objects.requireNonNull(value, "connectionTimeout");
    return validate(new ImmutableS3Instructions(
        this.regionName,
        this.maxConcurrentRequests,
        this.readAheadCount,
        this.fragmentSize,
        newValue,
        this.readTimeout,
        this.credentials,
        this.endpointOverride));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link S3Instructions#readTimeout() readTimeout} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for readTimeout
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableS3Instructions withReadTimeout(Duration value) {
    if (this.readTimeout == value) return this;
    Duration newValue = Objects.requireNonNull(value, "readTimeout");
    return validate(new ImmutableS3Instructions(
        this.regionName,
        this.maxConcurrentRequests,
        this.readAheadCount,
        this.fragmentSize,
        this.connectionTimeout,
        newValue,
        this.credentials,
        this.endpointOverride));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link S3Instructions#credentials() credentials} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for credentials
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableS3Instructions withCredentials(Credentials value) {
    if (this.credentials == value) return this;
    Credentials newValue = Objects.requireNonNull(value, "credentials");
    return validate(new ImmutableS3Instructions(
        this.regionName,
        this.maxConcurrentRequests,
        this.readAheadCount,
        this.fragmentSize,
        this.connectionTimeout,
        this.readTimeout,
        newValue,
        this.endpointOverride));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link S3Instructions#endpointOverride() endpointOverride} attribute.
   * @param value The value for endpointOverride
   * @return A modified copy of {@code this} object
   */
  public final ImmutableS3Instructions withEndpointOverride(URI value) {
    @Nullable URI newValue = Objects.requireNonNull(value, "endpointOverride");
    if (this.endpointOverride == newValue) return this;
    return validate(new ImmutableS3Instructions(
        this.regionName,
        this.maxConcurrentRequests,
        this.readAheadCount,
        this.fragmentSize,
        this.connectionTimeout,
        this.readTimeout,
        this.credentials,
        newValue));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link S3Instructions#endpointOverride() endpointOverride} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for endpointOverride
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableS3Instructions withEndpointOverride(Optional<? extends URI> optional) {
    @Nullable URI value = optional.orElse(null);
    if (this.endpointOverride == value) return this;
    return validate(new ImmutableS3Instructions(
        this.regionName,
        this.maxConcurrentRequests,
        this.readAheadCount,
        this.fragmentSize,
        this.connectionTimeout,
        this.readTimeout,
        this.credentials,
        value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableS3Instructions} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableS3Instructions
        && equalTo(0, (ImmutableS3Instructions) another);
  }

  private boolean equalTo(int synthetic, ImmutableS3Instructions another) {
    return Objects.equals(regionName, another.regionName)
        && maxConcurrentRequests == another.maxConcurrentRequests
        && readAheadCount == another.readAheadCount
        && fragmentSize == another.fragmentSize
        && connectionTimeout.equals(another.connectionTimeout)
        && readTimeout.equals(another.readTimeout)
        && credentials.equals(another.credentials)
        && Objects.equals(endpointOverride, another.endpointOverride);
  }

  /**
   * Computes a hash code from attributes: {@code regionName}, {@code maxConcurrentRequests}, {@code readAheadCount}, {@code fragmentSize}, {@code connectionTimeout}, {@code readTimeout}, {@code credentials}, {@code endpointOverride}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + Objects.hashCode(regionName);
    h += (h << 5) + maxConcurrentRequests;
    h += (h << 5) + readAheadCount;
    h += (h << 5) + fragmentSize;
    h += (h << 5) + connectionTimeout.hashCode();
    h += (h << 5) + readTimeout.hashCode();
    h += (h << 5) + credentials.hashCode();
    h += (h << 5) + Objects.hashCode(endpointOverride);
    return h;
  }

  /**
   * Prints the immutable value {@code S3Instructions} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("S3Instructions")
        .omitNullValues()
        .add("regionName", regionName)
        .add("maxConcurrentRequests", maxConcurrentRequests)
        .add("readAheadCount", readAheadCount)
        .add("fragmentSize", fragmentSize)
        .add("connectionTimeout", connectionTimeout)
        .add("readTimeout", readTimeout)
        .add("credentials", credentials)
        .add("endpointOverride", endpointOverride)
        .toString();
  }

  @SuppressWarnings("Immutable")
  private transient volatile long lazyInitBitmap;

  private static final long SINGLE_USE_LAZY_INIT_BIT = 0x1L;

  @SuppressWarnings("Immutable")
  private transient S3Instructions singleUse;

  /**
   * {@inheritDoc}
   * <p>
   * Returns a lazily initialized value of the {@link S3Instructions#singleUse() singleUse} attribute.
   * Initialized once and only once and stored for subsequent access with proper synchronization.
   * In case of any exception or error thrown by the lazy value initializer,
   * the result will not be memoised (i.e. remembered) and on next call computation
   * will be attempted again.
   * @return A lazily initialized value of the {@code singleUse} attribute
   */
  @Override
  S3Instructions singleUse() {
    if ((lazyInitBitmap & SINGLE_USE_LAZY_INIT_BIT) == 0) {
      synchronized (this) {
        if ((lazyInitBitmap & SINGLE_USE_LAZY_INIT_BIT) == 0) {
          this.singleUse = Objects.requireNonNull(super.singleUse(), "singleUse");
          lazyInitBitmap |= SINGLE_USE_LAZY_INIT_BIT;
        }
      }
    }
    return singleUse;
  }

  private static ImmutableS3Instructions validate(ImmutableS3Instructions instance) {
    instance.awsSdkV2Credentials();
    instance.boundsCheckMinFragmentSize();
    instance.boundsCheckReadAheadCount();
    instance.boundsCheckMaxConcurrentRequests();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link S3Instructions} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable S3Instructions instance
   */
  public static ImmutableS3Instructions copyOf(S3Instructions instance) {
    if (instance instanceof ImmutableS3Instructions) {
      return (ImmutableS3Instructions) instance;
    }
    return ImmutableS3Instructions.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableS3Instructions ImmutableS3Instructions}.
   * <pre>
   * ImmutableS3Instructions.builder()
   *    .regionName(String) // optional {@link S3Instructions#regionName() regionName}
   *    .maxConcurrentRequests(int) // optional {@link S3Instructions#maxConcurrentRequests() maxConcurrentRequests}
   *    .readAheadCount(int) // optional {@link S3Instructions#readAheadCount() readAheadCount}
   *    .fragmentSize(int) // optional {@link S3Instructions#fragmentSize() fragmentSize}
   *    .connectionTimeout(java.time.Duration) // optional {@link S3Instructions#connectionTimeout() connectionTimeout}
   *    .readTimeout(java.time.Duration) // optional {@link S3Instructions#readTimeout() readTimeout}
   *    .credentials(io.deephaven.extensions.s3.Credentials) // optional {@link S3Instructions#credentials() credentials}
   *    .endpointOverride(java.net.URI) // optional {@link S3Instructions#endpointOverride() endpointOverride}
   *    .build();
   * </pre>
   * @return A new ImmutableS3Instructions builder
   */
  public static ImmutableS3Instructions.Builder builder() {
    return new ImmutableS3Instructions.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableS3Instructions ImmutableS3Instructions}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "S3Instructions", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements S3Instructions.Builder {
    private static final long OPT_BIT_MAX_CONCURRENT_REQUESTS = 0x1L;
    private static final long OPT_BIT_READ_AHEAD_COUNT = 0x2L;
    private static final long OPT_BIT_FRAGMENT_SIZE = 0x4L;
    private long optBits;

    private @Nullable String regionName;
    private int maxConcurrentRequests;
    private int readAheadCount;
    private int fragmentSize;
    private @Nullable Duration connectionTimeout;
    private @Nullable Duration readTimeout;
    private @Nullable Credentials credentials;
    private @Nullable URI endpointOverride;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code S3Instructions} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(S3Instructions instance) {
      Objects.requireNonNull(instance, "instance");
      Optional<String> regionNameOptional = instance.regionName();
      if (regionNameOptional.isPresent()) {
        regionName(regionNameOptional);
      }
      maxConcurrentRequests(instance.maxConcurrentRequests());
      readAheadCount(instance.readAheadCount());
      fragmentSize(instance.fragmentSize());
      connectionTimeout(instance.connectionTimeout());
      readTimeout(instance.readTimeout());
      credentials(instance.credentials());
      Optional<URI> endpointOverrideOptional = instance.endpointOverride();
      if (endpointOverrideOptional.isPresent()) {
        endpointOverride(endpointOverrideOptional);
      }
      return this;
    }

    /**
     * Initializes the optional value {@link S3Instructions#regionName() regionName} to regionName.
     * @param regionName The value for regionName
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder regionName(String regionName) {
      this.regionName = Objects.requireNonNull(regionName, "regionName");
      return this;
    }

    /**
     * Initializes the optional value {@link S3Instructions#regionName() regionName} to regionName.
     * @param regionName The value for regionName
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder regionName(Optional<String> regionName) {
      this.regionName = regionName.orElse(null);
      return this;
    }

    /**
     * Initializes the value for the {@link S3Instructions#maxConcurrentRequests() maxConcurrentRequests} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link S3Instructions#maxConcurrentRequests() maxConcurrentRequests}.</em>
     * @param maxConcurrentRequests The value for maxConcurrentRequests 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder maxConcurrentRequests(int maxConcurrentRequests) {
      this.maxConcurrentRequests = maxConcurrentRequests;
      optBits |= OPT_BIT_MAX_CONCURRENT_REQUESTS;
      return this;
    }

    /**
     * Initializes the value for the {@link S3Instructions#readAheadCount() readAheadCount} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link S3Instructions#readAheadCount() readAheadCount}.</em>
     * @param readAheadCount The value for readAheadCount 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder readAheadCount(int readAheadCount) {
      this.readAheadCount = readAheadCount;
      optBits |= OPT_BIT_READ_AHEAD_COUNT;
      return this;
    }

    /**
     * Initializes the value for the {@link S3Instructions#fragmentSize() fragmentSize} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link S3Instructions#fragmentSize() fragmentSize}.</em>
     * @param fragmentSize The value for fragmentSize 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder fragmentSize(int fragmentSize) {
      this.fragmentSize = fragmentSize;
      optBits |= OPT_BIT_FRAGMENT_SIZE;
      return this;
    }

    /**
     * Initializes the value for the {@link S3Instructions#connectionTimeout() connectionTimeout} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link S3Instructions#connectionTimeout() connectionTimeout}.</em>
     * @param connectionTimeout The value for connectionTimeout 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder connectionTimeout(Duration connectionTimeout) {
      this.connectionTimeout = Objects.requireNonNull(connectionTimeout, "connectionTimeout");
      return this;
    }

    /**
     * Initializes the value for the {@link S3Instructions#readTimeout() readTimeout} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link S3Instructions#readTimeout() readTimeout}.</em>
     * @param readTimeout The value for readTimeout 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder readTimeout(Duration readTimeout) {
      this.readTimeout = Objects.requireNonNull(readTimeout, "readTimeout");
      return this;
    }

    /**
     * Initializes the value for the {@link S3Instructions#credentials() credentials} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link S3Instructions#credentials() credentials}.</em>
     * @param credentials The value for credentials 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder credentials(Credentials credentials) {
      this.credentials = Objects.requireNonNull(credentials, "credentials");
      return this;
    }

    /**
     * Initializes the optional value {@link S3Instructions#endpointOverride() endpointOverride} to endpointOverride.
     * @param endpointOverride The value for endpointOverride
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder endpointOverride(URI endpointOverride) {
      this.endpointOverride = Objects.requireNonNull(endpointOverride, "endpointOverride");
      return this;
    }

    /**
     * Initializes the optional value {@link S3Instructions#endpointOverride() endpointOverride} to endpointOverride.
     * @param endpointOverride The value for endpointOverride
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder endpointOverride(Optional<? extends URI> endpointOverride) {
      this.endpointOverride = endpointOverride.orElse(null);
      return this;
    }

    /**
     * Builds a new {@link ImmutableS3Instructions ImmutableS3Instructions}.
     * @return An immutable instance of S3Instructions
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableS3Instructions build() {
      return ImmutableS3Instructions.validate(new ImmutableS3Instructions(this));
    }

    private boolean maxConcurrentRequestsIsSet() {
      return (optBits & OPT_BIT_MAX_CONCURRENT_REQUESTS) != 0;
    }

    private boolean readAheadCountIsSet() {
      return (optBits & OPT_BIT_READ_AHEAD_COUNT) != 0;
    }

    private boolean fragmentSizeIsSet() {
      return (optBits & OPT_BIT_FRAGMENT_SIZE) != 0;
    }
  }
}
