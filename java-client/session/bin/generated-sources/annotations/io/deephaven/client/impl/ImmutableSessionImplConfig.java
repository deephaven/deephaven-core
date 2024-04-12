package io.deephaven.client.impl;

import com.google.common.base.MoreObjects;
import com.google.common.primitives.Booleans;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import io.deephaven.proto.DeephavenChannel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link SessionImplConfig}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableSessionImplConfig.builder()}.
 */
@Generated(from = "SessionImplConfig", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableSessionImplConfig extends SessionImplConfig {
  private final ScheduledExecutorService executor;
  private final DeephavenChannel channel;
  private final String authenticationTypeAndValue;
  private final boolean delegateToBatch;
  private final boolean mixinStacktrace;
  private final Duration executeTimeout;
  private final Duration closeTimeout;

  private ImmutableSessionImplConfig(ImmutableSessionImplConfig.Builder builder) {
    this.executor = builder.executor;
    this.channel = builder.channel;
    if (builder.authenticationTypeAndValue != null) {
      initShim.authenticationTypeAndValue(builder.authenticationTypeAndValue);
    }
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
    this.authenticationTypeAndValue = initShim.authenticationTypeAndValue();
    this.delegateToBatch = initShim.delegateToBatch();
    this.mixinStacktrace = initShim.mixinStacktrace();
    this.executeTimeout = initShim.executeTimeout();
    this.closeTimeout = initShim.closeTimeout();
    this.initShim = null;
  }

  private ImmutableSessionImplConfig(
      ScheduledExecutorService executor,
      DeephavenChannel channel,
      String authenticationTypeAndValue,
      boolean delegateToBatch,
      boolean mixinStacktrace,
      Duration executeTimeout,
      Duration closeTimeout) {
    this.executor = executor;
    this.channel = channel;
    this.authenticationTypeAndValue = authenticationTypeAndValue;
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

  @Generated(from = "SessionImplConfig", generator = "Immutables")
  private final class InitShim {
    private byte authenticationTypeAndValueBuildStage = STAGE_UNINITIALIZED;
    private String authenticationTypeAndValue;

    String authenticationTypeAndValue() {
      if (authenticationTypeAndValueBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (authenticationTypeAndValueBuildStage == STAGE_UNINITIALIZED) {
        authenticationTypeAndValueBuildStage = STAGE_INITIALIZING;
        this.authenticationTypeAndValue = Objects.requireNonNull(ImmutableSessionImplConfig.super.authenticationTypeAndValue(), "authenticationTypeAndValue");
        authenticationTypeAndValueBuildStage = STAGE_INITIALIZED;
      }
      return this.authenticationTypeAndValue;
    }

    void authenticationTypeAndValue(String authenticationTypeAndValue) {
      this.authenticationTypeAndValue = authenticationTypeAndValue;
      authenticationTypeAndValueBuildStage = STAGE_INITIALIZED;
    }

    private byte delegateToBatchBuildStage = STAGE_UNINITIALIZED;
    private boolean delegateToBatch;

    boolean delegateToBatch() {
      if (delegateToBatchBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (delegateToBatchBuildStage == STAGE_UNINITIALIZED) {
        delegateToBatchBuildStage = STAGE_INITIALIZING;
        this.delegateToBatch = ImmutableSessionImplConfig.super.delegateToBatch();
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
        this.mixinStacktrace = ImmutableSessionImplConfig.super.mixinStacktrace();
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
        this.executeTimeout = Objects.requireNonNull(ImmutableSessionImplConfig.super.executeTimeout(), "executeTimeout");
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
        this.closeTimeout = Objects.requireNonNull(ImmutableSessionImplConfig.super.closeTimeout(), "closeTimeout");
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
      if (authenticationTypeAndValueBuildStage == STAGE_INITIALIZING) attributes.add("authenticationTypeAndValue");
      if (delegateToBatchBuildStage == STAGE_INITIALIZING) attributes.add("delegateToBatch");
      if (mixinStacktraceBuildStage == STAGE_INITIALIZING) attributes.add("mixinStacktrace");
      if (executeTimeoutBuildStage == STAGE_INITIALIZING) attributes.add("executeTimeout");
      if (closeTimeoutBuildStage == STAGE_INITIALIZING) attributes.add("closeTimeout");
      return "Cannot build SessionImplConfig, attribute initializers form cycle " + attributes;
    }
  }

  /**
   * @return The value of the {@code executor} attribute
   */
  @Override
  public ScheduledExecutorService executor() {
    return executor;
  }

  /**
   * @return The value of the {@code channel} attribute
   */
  @Override
  public DeephavenChannel channel() {
    return channel;
  }

  /**
   * @return The value of the {@code authenticationTypeAndValue} attribute
   */
  @Override
  public String authenticationTypeAndValue() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.authenticationTypeAndValue()
        : this.authenticationTypeAndValue;
  }

  /**
   * Whether the {@link Session} implementation will implement a batch {@link TableHandleManager}. By default, is
   * {@code true}. The default can be overridden via the system property {@value DEEPHAVEN_SESSION_BATCH}.
   * @return true if the session will implement a batch manager, false if the session will implement a serial manager
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
   * {@value DEEPHAVEN_SESSION_BATCH_STACKTRACES}.
   * @return true if the default batch manager will mix-in stacktraces, false otherwise
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
   * {@value DEEPHAVEN_SESSION_EXECUTE_TIMEOUT}.
   * @return the session execute timeout
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
   * overridden via the system property {@value DEEPHAVEN_SESSION_CLOSE_TIMEOUT}.
   * @return the close timeout
   */
  @Override
  public Duration closeTimeout() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.closeTimeout()
        : this.closeTimeout;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SessionImplConfig#executor() executor} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for executor
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSessionImplConfig withExecutor(ScheduledExecutorService value) {
    if (this.executor == value) return this;
    ScheduledExecutorService newValue = Objects.requireNonNull(value, "executor");
    return new ImmutableSessionImplConfig(
        newValue,
        this.channel,
        this.authenticationTypeAndValue,
        this.delegateToBatch,
        this.mixinStacktrace,
        this.executeTimeout,
        this.closeTimeout);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SessionImplConfig#channel() channel} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for channel
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSessionImplConfig withChannel(DeephavenChannel value) {
    if (this.channel == value) return this;
    DeephavenChannel newValue = Objects.requireNonNull(value, "channel");
    return new ImmutableSessionImplConfig(
        this.executor,
        newValue,
        this.authenticationTypeAndValue,
        this.delegateToBatch,
        this.mixinStacktrace,
        this.executeTimeout,
        this.closeTimeout);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SessionImplConfig#authenticationTypeAndValue() authenticationTypeAndValue} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for authenticationTypeAndValue
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSessionImplConfig withAuthenticationTypeAndValue(String value) {
    String newValue = Objects.requireNonNull(value, "authenticationTypeAndValue");
    if (this.authenticationTypeAndValue.equals(newValue)) return this;
    return new ImmutableSessionImplConfig(
        this.executor,
        this.channel,
        newValue,
        this.delegateToBatch,
        this.mixinStacktrace,
        this.executeTimeout,
        this.closeTimeout);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SessionImplConfig#delegateToBatch() delegateToBatch} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for delegateToBatch
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSessionImplConfig withDelegateToBatch(boolean value) {
    if (this.delegateToBatch == value) return this;
    return new ImmutableSessionImplConfig(
        this.executor,
        this.channel,
        this.authenticationTypeAndValue,
        value,
        this.mixinStacktrace,
        this.executeTimeout,
        this.closeTimeout);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SessionImplConfig#mixinStacktrace() mixinStacktrace} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for mixinStacktrace
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSessionImplConfig withMixinStacktrace(boolean value) {
    if (this.mixinStacktrace == value) return this;
    return new ImmutableSessionImplConfig(
        this.executor,
        this.channel,
        this.authenticationTypeAndValue,
        this.delegateToBatch,
        value,
        this.executeTimeout,
        this.closeTimeout);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SessionImplConfig#executeTimeout() executeTimeout} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for executeTimeout
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSessionImplConfig withExecuteTimeout(Duration value) {
    if (this.executeTimeout == value) return this;
    Duration newValue = Objects.requireNonNull(value, "executeTimeout");
    return new ImmutableSessionImplConfig(
        this.executor,
        this.channel,
        this.authenticationTypeAndValue,
        this.delegateToBatch,
        this.mixinStacktrace,
        newValue,
        this.closeTimeout);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SessionImplConfig#closeTimeout() closeTimeout} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for closeTimeout
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSessionImplConfig withCloseTimeout(Duration value) {
    if (this.closeTimeout == value) return this;
    Duration newValue = Objects.requireNonNull(value, "closeTimeout");
    return new ImmutableSessionImplConfig(
        this.executor,
        this.channel,
        this.authenticationTypeAndValue,
        this.delegateToBatch,
        this.mixinStacktrace,
        this.executeTimeout,
        newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableSessionImplConfig} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableSessionImplConfig
        && equalTo(0, (ImmutableSessionImplConfig) another);
  }

  private boolean equalTo(int synthetic, ImmutableSessionImplConfig another) {
    return executor.equals(another.executor)
        && channel.equals(another.channel)
        && authenticationTypeAndValue.equals(another.authenticationTypeAndValue)
        && delegateToBatch == another.delegateToBatch
        && mixinStacktrace == another.mixinStacktrace
        && executeTimeout.equals(another.executeTimeout)
        && closeTimeout.equals(another.closeTimeout);
  }

  /**
   * Computes a hash code from attributes: {@code executor}, {@code channel}, {@code authenticationTypeAndValue}, {@code delegateToBatch}, {@code mixinStacktrace}, {@code executeTimeout}, {@code closeTimeout}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + executor.hashCode();
    h += (h << 5) + channel.hashCode();
    h += (h << 5) + authenticationTypeAndValue.hashCode();
    h += (h << 5) + Booleans.hashCode(delegateToBatch);
    h += (h << 5) + Booleans.hashCode(mixinStacktrace);
    h += (h << 5) + executeTimeout.hashCode();
    h += (h << 5) + closeTimeout.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code SessionImplConfig} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("SessionImplConfig")
        .omitNullValues()
        .add("executor", executor)
        .add("channel", channel)
        .add("authenticationTypeAndValue", authenticationTypeAndValue)
        .add("delegateToBatch", delegateToBatch)
        .add("mixinStacktrace", mixinStacktrace)
        .add("executeTimeout", executeTimeout)
        .add("closeTimeout", closeTimeout)
        .toString();
  }

  /**
   * Creates an immutable copy of a {@link SessionImplConfig} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable SessionImplConfig instance
   */
  public static ImmutableSessionImplConfig copyOf(SessionImplConfig instance) {
    if (instance instanceof ImmutableSessionImplConfig) {
      return (ImmutableSessionImplConfig) instance;
    }
    return ImmutableSessionImplConfig.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableSessionImplConfig ImmutableSessionImplConfig}.
   * <pre>
   * ImmutableSessionImplConfig.builder()
   *    .executor(concurrent.ScheduledExecutorService) // required {@link SessionImplConfig#executor() executor}
   *    .channel(io.deephaven.proto.DeephavenChannel) // required {@link SessionImplConfig#channel() channel}
   *    .authenticationTypeAndValue(String) // optional {@link SessionImplConfig#authenticationTypeAndValue() authenticationTypeAndValue}
   *    .delegateToBatch(boolean) // optional {@link SessionImplConfig#delegateToBatch() delegateToBatch}
   *    .mixinStacktrace(boolean) // optional {@link SessionImplConfig#mixinStacktrace() mixinStacktrace}
   *    .executeTimeout(java.time.Duration) // optional {@link SessionImplConfig#executeTimeout() executeTimeout}
   *    .closeTimeout(java.time.Duration) // optional {@link SessionImplConfig#closeTimeout() closeTimeout}
   *    .build();
   * </pre>
   * @return A new ImmutableSessionImplConfig builder
   */
  public static ImmutableSessionImplConfig.Builder builder() {
    return new ImmutableSessionImplConfig.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableSessionImplConfig ImmutableSessionImplConfig}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "SessionImplConfig", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements SessionImplConfig.Builder {
    private static final long INIT_BIT_EXECUTOR = 0x1L;
    private static final long INIT_BIT_CHANNEL = 0x2L;
    private static final long OPT_BIT_DELEGATE_TO_BATCH = 0x1L;
    private static final long OPT_BIT_MIXIN_STACKTRACE = 0x2L;
    private long initBits = 0x3L;
    private long optBits;

    private @Nullable ScheduledExecutorService executor;
    private @Nullable DeephavenChannel channel;
    private @Nullable String authenticationTypeAndValue;
    private boolean delegateToBatch;
    private boolean mixinStacktrace;
    private @Nullable Duration executeTimeout;
    private @Nullable Duration closeTimeout;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code SessionImplConfig} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(SessionImplConfig instance) {
      Objects.requireNonNull(instance, "instance");
      executor(instance.executor());
      channel(instance.channel());
      authenticationTypeAndValue(instance.authenticationTypeAndValue());
      delegateToBatch(instance.delegateToBatch());
      mixinStacktrace(instance.mixinStacktrace());
      executeTimeout(instance.executeTimeout());
      closeTimeout(instance.closeTimeout());
      return this;
    }

    /**
     * Initializes the value for the {@link SessionImplConfig#executor() executor} attribute.
     * @param executor The value for executor 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder executor(ScheduledExecutorService executor) {
      this.executor = Objects.requireNonNull(executor, "executor");
      initBits &= ~INIT_BIT_EXECUTOR;
      return this;
    }

    /**
     * Initializes the value for the {@link SessionImplConfig#channel() channel} attribute.
     * @param channel The value for channel 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder channel(DeephavenChannel channel) {
      this.channel = Objects.requireNonNull(channel, "channel");
      initBits &= ~INIT_BIT_CHANNEL;
      return this;
    }

    /**
     * Initializes the value for the {@link SessionImplConfig#authenticationTypeAndValue() authenticationTypeAndValue} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link SessionImplConfig#authenticationTypeAndValue() authenticationTypeAndValue}.</em>
     * @param authenticationTypeAndValue The value for authenticationTypeAndValue 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder authenticationTypeAndValue(String authenticationTypeAndValue) {
      this.authenticationTypeAndValue = Objects.requireNonNull(authenticationTypeAndValue, "authenticationTypeAndValue");
      return this;
    }

    /**
     * Initializes the value for the {@link SessionImplConfig#delegateToBatch() delegateToBatch} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link SessionImplConfig#delegateToBatch() delegateToBatch}.</em>
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
     * Initializes the value for the {@link SessionImplConfig#mixinStacktrace() mixinStacktrace} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link SessionImplConfig#mixinStacktrace() mixinStacktrace}.</em>
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
     * Initializes the value for the {@link SessionImplConfig#executeTimeout() executeTimeout} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link SessionImplConfig#executeTimeout() executeTimeout}.</em>
     * @param executeTimeout The value for executeTimeout 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder executeTimeout(Duration executeTimeout) {
      this.executeTimeout = Objects.requireNonNull(executeTimeout, "executeTimeout");
      return this;
    }

    /**
     * Initializes the value for the {@link SessionImplConfig#closeTimeout() closeTimeout} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link SessionImplConfig#closeTimeout() closeTimeout}.</em>
     * @param closeTimeout The value for closeTimeout 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder closeTimeout(Duration closeTimeout) {
      this.closeTimeout = Objects.requireNonNull(closeTimeout, "closeTimeout");
      return this;
    }

    /**
     * Builds a new {@link ImmutableSessionImplConfig ImmutableSessionImplConfig}.
     * @return An immutable instance of SessionImplConfig
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableSessionImplConfig build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableSessionImplConfig(this);
    }

    private boolean delegateToBatchIsSet() {
      return (optBits & OPT_BIT_DELEGATE_TO_BATCH) != 0;
    }

    private boolean mixinStacktraceIsSet() {
      return (optBits & OPT_BIT_MIXIN_STACKTRACE) != 0;
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_EXECUTOR) != 0) attributes.add("executor");
      if ((initBits & INIT_BIT_CHANNEL) != 0) attributes.add("channel");
      return "Cannot build SessionImplConfig, some of required attributes are not set " + attributes;
    }
  }
}
