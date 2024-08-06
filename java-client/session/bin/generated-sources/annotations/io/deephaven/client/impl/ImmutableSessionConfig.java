package io.deephaven.client.impl;

import com.google.common.base.MoreObjects;
import com.google.common.primitives.Booleans;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link SessionConfig}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableSessionConfig.builder()}.
 */
@Generated(from = "SessionConfig", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableSessionConfig extends SessionConfig {
  private final @Nullable String authenticationTypeAndValue;
  private final @Nullable ScheduledExecutorService scheduler;
  private final boolean delegateToBatch;
  private final boolean mixinStacktrace;
  private final Duration executeTimeout;
  private final Duration closeTimeout;

  private ImmutableSessionConfig(ImmutableSessionConfig.Builder builder) {
    this.authenticationTypeAndValue = builder.authenticationTypeAndValue;
    this.scheduler = builder.scheduler;
    if (builder.delegateToBatchIsSet()) {
      initShim.delegateToBatch(builder.delegateToBatch);
    }
    if (builder.mixinStacktraceIsSet()) {
      initShim.mixinStacktrace(builder.mixinStacktrace);
    }
    if (builder.executeTimeout != null) {
      initShim.executeTimeout(builder.executeTimeout);
    }
    if (builder.closeTimeout != null) {
      initShim.closeTimeout(builder.closeTimeout);
    }
    this.delegateToBatch = initShim.delegateToBatch();
    this.mixinStacktrace = initShim.mixinStacktrace();
    this.executeTimeout = initShim.executeTimeout();
    this.closeTimeout = initShim.closeTimeout();
    this.initShim = null;
  }

  private ImmutableSessionConfig(
      @Nullable String authenticationTypeAndValue,
      @Nullable ScheduledExecutorService scheduler,
      boolean delegateToBatch,
      boolean mixinStacktrace,
      Duration executeTimeout,
      Duration closeTimeout) {
    this.authenticationTypeAndValue = authenticationTypeAndValue;
    this.scheduler = scheduler;
    this.delegateToBatch = delegateToBatch;
    this.mixinStacktrace = mixinStacktrace;
    this.executeTimeout = executeTimeout;
    this.closeTimeout = closeTimeout;
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  @SuppressWarnings("Immutable")
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "SessionConfig", generator = "Immutables")
  private final class InitShim {
    private byte delegateToBatchBuildStage = STAGE_UNINITIALIZED;
    private boolean delegateToBatch;

    boolean delegateToBatch() {
      if (delegateToBatchBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (delegateToBatchBuildStage == STAGE_UNINITIALIZED) {
        delegateToBatchBuildStage = STAGE_INITIALIZING;
        this.delegateToBatch = ImmutableSessionConfig.super.delegateToBatch();
        delegateToBatchBuildStage = STAGE_INITIALIZED;
      }
      return this.delegateToBatch;
    }

    void delegateToBatch(boolean delegateToBatch) {
      this.delegateToBatch = delegateToBatch;
      delegateToBatchBuildStage = STAGE_INITIALIZED;
    }

    private byte mixinStacktraceBuildStage = STAGE_UNINITIALIZED;
    private boolean mixinStacktrace;

    boolean mixinStacktrace() {
      if (mixinStacktraceBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (mixinStacktraceBuildStage == STAGE_UNINITIALIZED) {
        mixinStacktraceBuildStage = STAGE_INITIALIZING;
        this.mixinStacktrace = ImmutableSessionConfig.super.mixinStacktrace();
        mixinStacktraceBuildStage = STAGE_INITIALIZED;
      }
      return this.mixinStacktrace;
    }

    void mixinStacktrace(boolean mixinStacktrace) {
      this.mixinStacktrace = mixinStacktrace;
      mixinStacktraceBuildStage = STAGE_INITIALIZED;
    }

    private byte executeTimeoutBuildStage = STAGE_UNINITIALIZED;
    private Duration executeTimeout;

    Duration executeTimeout() {
      if (executeTimeoutBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (executeTimeoutBuildStage == STAGE_UNINITIALIZED) {
        executeTimeoutBuildStage = STAGE_INITIALIZING;
        this.executeTimeout = Objects.requireNonNull(ImmutableSessionConfig.super.executeTimeout(), "executeTimeout");
        executeTimeoutBuildStage = STAGE_INITIALIZED;
      }
      return this.executeTimeout;
    }

    void executeTimeout(Duration executeTimeout) {
      this.executeTimeout = executeTimeout;
      executeTimeoutBuildStage = STAGE_INITIALIZED;
    }

    private byte closeTimeoutBuildStage = STAGE_UNINITIALIZED;
    private Duration closeTimeout;

    Duration closeTimeout() {
      if (closeTimeoutBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (closeTimeoutBuildStage == STAGE_UNINITIALIZED) {
        closeTimeoutBuildStage = STAGE_INITIALIZING;
        this.closeTimeout = Objects.requireNonNull(ImmutableSessionConfig.super.closeTimeout(), "closeTimeout");
        closeTimeoutBuildStage = STAGE_INITIALIZED;
      }
      return this.closeTimeout;
    }

    void closeTimeout(Duration closeTimeout) {
      this.closeTimeout = closeTimeout;
      closeTimeoutBuildStage = STAGE_INITIALIZED;
    }

    private String formatInitCycleMessage() {
      List<String> attributes = new ArrayList<>();
      if (delegateToBatchBuildStage == STAGE_INITIALIZING) attributes.add("delegateToBatch");
      if (mixinStacktraceBuildStage == STAGE_INITIALIZING) attributes.add("mixinStacktrace");
      if (executeTimeoutBuildStage == STAGE_INITIALIZING) attributes.add("executeTimeout");
      if (closeTimeoutBuildStage == STAGE_INITIALIZING) attributes.add("closeTimeout");
      return "Cannot build SessionConfig, attribute initializers form cycle " + attributes;
    }
  }

  /**
   * The authentication type and value.
   */
  @Override
  public Optional<String> authenticationTypeAndValue() {
    return Optional.ofNullable(authenticationTypeAndValue);
  }

  /**
   * The scheduler.
   */
  @Override
  public Optional<ScheduledExecutorService> scheduler() {
    return Optional.ofNullable(scheduler);
  }

  /**
   * Whether the {@link Session} implementation will implement a batch {@link TableHandleManager}. By default, is
   * {@code true}. The default can be overridden via the system property
   * {@value SessionImplConfig#DEEPHAVEN_SESSION_BATCH}.
   */
  @Override
  public boolean delegateToBatch() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.delegateToBatch()
        : this.delegateToBatch;
  }

  /**
   * Whether the default batch {@link TableHandleManager} will use mix-in more relevant stacktraces. By default, is
   * {@code false}. The default can be overridden via the system property
   * {@value SessionImplConfig#DEEPHAVEN_SESSION_BATCH_STACKTRACES}.
   */
  @Override
  public boolean mixinStacktrace() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.mixinStacktrace()
        : this.mixinStacktrace;
  }

  /**
   * The session execute timeout. By default, is {@code PT1m}. The default can be overridden via the system property
   * {@value SessionImplConfig#DEEPHAVEN_SESSION_EXECUTE_TIMEOUT}.
   */
  @Override
  public Duration executeTimeout() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.executeTimeout()
        : this.executeTimeout;
  }

  /**
   * The {@link Session} and {@link ConsoleSession} close timeout. By default, is {@code PT5s}. The default can be
   * overridden via the system property {@value SessionImplConfig#DEEPHAVEN_SESSION_CLOSE_TIMEOUT}.
   */
  @Override
  public Duration closeTimeout() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.closeTimeout()
        : this.closeTimeout;
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link SessionConfig#authenticationTypeAndValue() authenticationTypeAndValue} attribute.
   * @param value The value for authenticationTypeAndValue
   * @return A modified copy of {@code this} object
   */
  public final ImmutableSessionConfig withAuthenticationTypeAndValue(String value) {
    @Nullable String newValue = Objects.requireNonNull(value, "authenticationTypeAndValue");
    if (Objects.equals(this.authenticationTypeAndValue, newValue)) return this;
    return new ImmutableSessionConfig(
        newValue,
        this.scheduler,
        this.delegateToBatch,
        this.mixinStacktrace,
        this.executeTimeout,
        this.closeTimeout);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link SessionConfig#authenticationTypeAndValue() authenticationTypeAndValue} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for authenticationTypeAndValue
   * @return A modified copy of {@code this} object
   */
  public final ImmutableSessionConfig withAuthenticationTypeAndValue(Optional<String> optional) {
    @Nullable String value = optional.orElse(null);
    if (Objects.equals(this.authenticationTypeAndValue, value)) return this;
    return new ImmutableSessionConfig(
        value,
        this.scheduler,
        this.delegateToBatch,
        this.mixinStacktrace,
        this.executeTimeout,
        this.closeTimeout);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link SessionConfig#scheduler() scheduler} attribute.
   * @param value The value for scheduler
   * @return A modified copy of {@code this} object
   */
  public final ImmutableSessionConfig withScheduler(ScheduledExecutorService value) {
    @Nullable ScheduledExecutorService newValue = Objects.requireNonNull(value, "scheduler");
    if (this.scheduler == newValue) return this;
    return new ImmutableSessionConfig(
        this.authenticationTypeAndValue,
        newValue,
        this.delegateToBatch,
        this.mixinStacktrace,
        this.executeTimeout,
        this.closeTimeout);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link SessionConfig#scheduler() scheduler} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for scheduler
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableSessionConfig withScheduler(Optional<? extends ScheduledExecutorService> optional) {
    @Nullable ScheduledExecutorService value = optional.orElse(null);
    if (this.scheduler == value) return this;
    return new ImmutableSessionConfig(
        this.authenticationTypeAndValue,
        value,
        this.delegateToBatch,
        this.mixinStacktrace,
        this.executeTimeout,
        this.closeTimeout);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SessionConfig#delegateToBatch() delegateToBatch} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for delegateToBatch
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSessionConfig withDelegateToBatch(boolean value) {
    if (this.delegateToBatch == value) return this;
    return new ImmutableSessionConfig(
        this.authenticationTypeAndValue,
        this.scheduler,
        value,
        this.mixinStacktrace,
        this.executeTimeout,
        this.closeTimeout);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SessionConfig#mixinStacktrace() mixinStacktrace} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for mixinStacktrace
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSessionConfig withMixinStacktrace(boolean value) {
    if (this.mixinStacktrace == value) return this;
    return new ImmutableSessionConfig(
        this.authenticationTypeAndValue,
        this.scheduler,
        this.delegateToBatch,
        value,
        this.executeTimeout,
        this.closeTimeout);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SessionConfig#executeTimeout() executeTimeout} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for executeTimeout
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSessionConfig withExecuteTimeout(Duration value) {
    if (this.executeTimeout == value) return this;
    Duration newValue = Objects.requireNonNull(value, "executeTimeout");
    return new ImmutableSessionConfig(
        this.authenticationTypeAndValue,
        this.scheduler,
        this.delegateToBatch,
        this.mixinStacktrace,
        newValue,
        this.closeTimeout);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SessionConfig#closeTimeout() closeTimeout} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for closeTimeout
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSessionConfig withCloseTimeout(Duration value) {
    if (this.closeTimeout == value) return this;
    Duration newValue = Objects.requireNonNull(value, "closeTimeout");
    return new ImmutableSessionConfig(
        this.authenticationTypeAndValue,
        this.scheduler,
        this.delegateToBatch,
        this.mixinStacktrace,
        this.executeTimeout,
        newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableSessionConfig} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableSessionConfig
        && equalTo(0, (ImmutableSessionConfig) another);
  }

  private boolean equalTo(int synthetic, ImmutableSessionConfig another) {
    return Objects.equals(authenticationTypeAndValue, another.authenticationTypeAndValue)
        && Objects.equals(scheduler, another.scheduler)
        && delegateToBatch == another.delegateToBatch
        && mixinStacktrace == another.mixinStacktrace
        && executeTimeout.equals(another.executeTimeout)
        && closeTimeout.equals(another.closeTimeout);
  }

  /**
   * Computes a hash code from attributes: {@code authenticationTypeAndValue}, {@code scheduler}, {@code delegateToBatch}, {@code mixinStacktrace}, {@code executeTimeout}, {@code closeTimeout}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + Objects.hashCode(authenticationTypeAndValue);
    h += (h << 5) + Objects.hashCode(scheduler);
    h += (h << 5) + Booleans.hashCode(delegateToBatch);
    h += (h << 5) + Booleans.hashCode(mixinStacktrace);
    h += (h << 5) + executeTimeout.hashCode();
    h += (h << 5) + closeTimeout.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code SessionConfig} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("SessionConfig")
        .omitNullValues()
        .add("scheduler", scheduler)
        .add("delegateToBatch", delegateToBatch)
        .add("mixinStacktrace", mixinStacktrace)
        .add("executeTimeout", executeTimeout)
        .add("closeTimeout", closeTimeout)
        .toString();
  }

  /**
   * Creates an immutable copy of a {@link SessionConfig} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable SessionConfig instance
   */
  public static ImmutableSessionConfig copyOf(SessionConfig instance) {
    if (instance instanceof ImmutableSessionConfig) {
      return (ImmutableSessionConfig) instance;
    }
    return ImmutableSessionConfig.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableSessionConfig ImmutableSessionConfig}.
   * <pre>
   * ImmutableSessionConfig.builder()
   *    .authenticationTypeAndValue(String) // optional {@link SessionConfig#authenticationTypeAndValue() authenticationTypeAndValue}
   *    .scheduler(concurrent.ScheduledExecutorService) // optional {@link SessionConfig#scheduler() scheduler}
   *    .delegateToBatch(boolean) // optional {@link SessionConfig#delegateToBatch() delegateToBatch}
   *    .mixinStacktrace(boolean) // optional {@link SessionConfig#mixinStacktrace() mixinStacktrace}
   *    .executeTimeout(java.time.Duration) // optional {@link SessionConfig#executeTimeout() executeTimeout}
   *    .closeTimeout(java.time.Duration) // optional {@link SessionConfig#closeTimeout() closeTimeout}
   *    .build();
   * </pre>
   * @return A new ImmutableSessionConfig builder
   */
  public static ImmutableSessionConfig.Builder builder() {
    return new ImmutableSessionConfig.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableSessionConfig ImmutableSessionConfig}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "SessionConfig", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements SessionConfig.Builder {
    private static final long OPT_BIT_DELEGATE_TO_BATCH = 0x1L;
    private static final long OPT_BIT_MIXIN_STACKTRACE = 0x2L;
    private long optBits;

    private @Nullable String authenticationTypeAndValue;
    private @Nullable ScheduledExecutorService scheduler;
    private boolean delegateToBatch;
    private boolean mixinStacktrace;
    private @Nullable Duration executeTimeout;
    private @Nullable Duration closeTimeout;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code SessionConfig} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(SessionConfig instance) {
      Objects.requireNonNull(instance, "instance");
      Optional<String> authenticationTypeAndValueOptional = instance.authenticationTypeAndValue();
      if (authenticationTypeAndValueOptional.isPresent()) {
        authenticationTypeAndValue(authenticationTypeAndValueOptional);
      }
      Optional<ScheduledExecutorService> schedulerOptional = instance.scheduler();
      if (schedulerOptional.isPresent()) {
        scheduler(schedulerOptional);
      }
      delegateToBatch(instance.delegateToBatch());
      mixinStacktrace(instance.mixinStacktrace());
      executeTimeout(instance.executeTimeout());
      closeTimeout(instance.closeTimeout());
      return this;
    }

    /**
     * Initializes the optional value {@link SessionConfig#authenticationTypeAndValue() authenticationTypeAndValue} to authenticationTypeAndValue.
     * @param authenticationTypeAndValue The value for authenticationTypeAndValue
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder authenticationTypeAndValue(String authenticationTypeAndValue) {
      this.authenticationTypeAndValue = Objects.requireNonNull(authenticationTypeAndValue, "authenticationTypeAndValue");
      return this;
    }

    /**
     * Initializes the optional value {@link SessionConfig#authenticationTypeAndValue() authenticationTypeAndValue} to authenticationTypeAndValue.
     * @param authenticationTypeAndValue The value for authenticationTypeAndValue
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder authenticationTypeAndValue(Optional<String> authenticationTypeAndValue) {
      this.authenticationTypeAndValue = authenticationTypeAndValue.orElse(null);
      return this;
    }

    /**
     * Initializes the optional value {@link SessionConfig#scheduler() scheduler} to scheduler.
     * @param scheduler The value for scheduler
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder scheduler(ScheduledExecutorService scheduler) {
      this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
      return this;
    }

    /**
     * Initializes the optional value {@link SessionConfig#scheduler() scheduler} to scheduler.
     * @param scheduler The value for scheduler
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder scheduler(Optional<? extends ScheduledExecutorService> scheduler) {
      this.scheduler = scheduler.orElse(null);
      return this;
    }

    /**
     * Initializes the value for the {@link SessionConfig#delegateToBatch() delegateToBatch} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link SessionConfig#delegateToBatch() delegateToBatch}.</em>
     * @param delegateToBatch The value for delegateToBatch 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder delegateToBatch(boolean delegateToBatch) {
      this.delegateToBatch = delegateToBatch;
      optBits |= OPT_BIT_DELEGATE_TO_BATCH;
      return this;
    }

    /**
     * Initializes the value for the {@link SessionConfig#mixinStacktrace() mixinStacktrace} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link SessionConfig#mixinStacktrace() mixinStacktrace}.</em>
     * @param mixinStacktrace The value for mixinStacktrace 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder mixinStacktrace(boolean mixinStacktrace) {
      this.mixinStacktrace = mixinStacktrace;
      optBits |= OPT_BIT_MIXIN_STACKTRACE;
      return this;
    }

    /**
     * Initializes the value for the {@link SessionConfig#executeTimeout() executeTimeout} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link SessionConfig#executeTimeout() executeTimeout}.</em>
     * @param executeTimeout The value for executeTimeout 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder executeTimeout(Duration executeTimeout) {
      this.executeTimeout = Objects.requireNonNull(executeTimeout, "executeTimeout");
      return this;
    }

    /**
     * Initializes the value for the {@link SessionConfig#closeTimeout() closeTimeout} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link SessionConfig#closeTimeout() closeTimeout}.</em>
     * @param closeTimeout The value for closeTimeout 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder closeTimeout(Duration closeTimeout) {
      this.closeTimeout = Objects.requireNonNull(closeTimeout, "closeTimeout");
      return this;
    }

    /**
     * Builds a new {@link ImmutableSessionConfig ImmutableSessionConfig}.
     * @return An immutable instance of SessionConfig
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableSessionConfig build() {
      return new ImmutableSessionConfig(this);
    }

    private boolean delegateToBatchIsSet() {
      return (optBits & OPT_BIT_DELEGATE_TO_BATCH) != 0;
    }

    private boolean mixinStacktraceIsSet() {
      return (optBits & OPT_BIT_MIXIN_STACKTRACE) != 0;
    }
  }
}
