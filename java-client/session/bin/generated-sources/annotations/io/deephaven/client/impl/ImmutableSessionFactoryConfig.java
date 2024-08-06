package io.deephaven.client.impl;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
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
 * Immutable implementation of {@link SessionFactoryConfig}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableSessionFactoryConfig.builder()}.
 */
@Generated(from = "SessionFactoryConfig", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableSessionFactoryConfig extends SessionFactoryConfig {
  private final ClientConfig clientConfig;
  private final ClientChannelFactory clientChannelFactory;
  private final SessionConfig sessionConfig;
  private final ScheduledExecutorService scheduler;

  private ImmutableSessionFactoryConfig(ImmutableSessionFactoryConfig.Builder builder) {
    this.clientConfig = builder.clientConfig;
    this.scheduler = builder.scheduler;
    if (builder.clientChannelFactory != null) {
      initShim.clientChannelFactory(builder.clientChannelFactory);
    }
    if (builder.sessionConfig != null) {
      initShim.sessionConfig(builder.sessionConfig);
    }
    this.clientChannelFactory = initShim.clientChannelFactory();
    this.sessionConfig = initShim.sessionConfig();
    this.initShim = null;
  }

  private ImmutableSessionFactoryConfig(
      ClientConfig clientConfig,
      ClientChannelFactory clientChannelFactory,
      SessionConfig sessionConfig,
      ScheduledExecutorService scheduler) {
    this.clientConfig = clientConfig;
    this.clientChannelFactory = clientChannelFactory;
    this.sessionConfig = sessionConfig;
    this.scheduler = scheduler;
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  @SuppressWarnings("Immutable")
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "SessionFactoryConfig", generator = "Immutables")
  private final class InitShim {
    private byte clientChannelFactoryBuildStage = STAGE_UNINITIALIZED;
    private ClientChannelFactory clientChannelFactory;

    ClientChannelFactory clientChannelFactory() {
      if (clientChannelFactoryBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (clientChannelFactoryBuildStage == STAGE_UNINITIALIZED) {
        clientChannelFactoryBuildStage = STAGE_INITIALIZING;
        this.clientChannelFactory = Objects.requireNonNull(ImmutableSessionFactoryConfig.super.clientChannelFactory(), "clientChannelFactory");
        clientChannelFactoryBuildStage = STAGE_INITIALIZED;
      }
      return this.clientChannelFactory;
    }

    void clientChannelFactory(ClientChannelFactory clientChannelFactory) {
      this.clientChannelFactory = clientChannelFactory;
      clientChannelFactoryBuildStage = STAGE_INITIALIZED;
    }

    private byte sessionConfigBuildStage = STAGE_UNINITIALIZED;
    private SessionConfig sessionConfig;

    SessionConfig sessionConfig() {
      if (sessionConfigBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (sessionConfigBuildStage == STAGE_UNINITIALIZED) {
        sessionConfigBuildStage = STAGE_INITIALIZING;
        this.sessionConfig = Objects.requireNonNull(ImmutableSessionFactoryConfig.super.sessionConfig(), "sessionConfig");
        sessionConfigBuildStage = STAGE_INITIALIZED;
      }
      return this.sessionConfig;
    }

    void sessionConfig(SessionConfig sessionConfig) {
      this.sessionConfig = sessionConfig;
      sessionConfigBuildStage = STAGE_INITIALIZED;
    }

    private String formatInitCycleMessage() {
      List<String> attributes = new ArrayList<>();
      if (clientChannelFactoryBuildStage == STAGE_INITIALIZING) attributes.add("clientChannelFactory");
      if (sessionConfigBuildStage == STAGE_INITIALIZING) attributes.add("sessionConfig");
      return "Cannot build SessionFactoryConfig, attribute initializers form cycle " + attributes;
    }
  }

  /**
   * The client configuration.
   */
  @Override
  public ClientConfig clientConfig() {
    return clientConfig;
  }

  /**
   * The client channel factory. By default is {@link ClientChannelFactory#defaultInstance()}.
   */
  @Override
  public ClientChannelFactory clientChannelFactory() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.clientChannelFactory()
        : this.clientChannelFactory;
  }

  /**
   * The default session config, used by the factory when {@link SessionConfig} is not provided. By default is
   * {@code SessionConfig.builder().build()}.
   */
  @Override
  public SessionConfig sessionConfig() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.sessionConfig()
        : this.sessionConfig;
  }

  /**
   * The default scheduler, used by the factory when {@link SessionConfig#scheduler()} is not set.
   */
  @Override
  public ScheduledExecutorService scheduler() {
    return scheduler;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SessionFactoryConfig#clientConfig() clientConfig} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for clientConfig
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSessionFactoryConfig withClientConfig(ClientConfig value) {
    if (this.clientConfig == value) return this;
    ClientConfig newValue = Objects.requireNonNull(value, "clientConfig");
    return new ImmutableSessionFactoryConfig(newValue, this.clientChannelFactory, this.sessionConfig, this.scheduler);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SessionFactoryConfig#clientChannelFactory() clientChannelFactory} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for clientChannelFactory
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSessionFactoryConfig withClientChannelFactory(ClientChannelFactory value) {
    if (this.clientChannelFactory == value) return this;
    ClientChannelFactory newValue = Objects.requireNonNull(value, "clientChannelFactory");
    return new ImmutableSessionFactoryConfig(this.clientConfig, newValue, this.sessionConfig, this.scheduler);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SessionFactoryConfig#sessionConfig() sessionConfig} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for sessionConfig
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSessionFactoryConfig withSessionConfig(SessionConfig value) {
    if (this.sessionConfig == value) return this;
    SessionConfig newValue = Objects.requireNonNull(value, "sessionConfig");
    return new ImmutableSessionFactoryConfig(this.clientConfig, this.clientChannelFactory, newValue, this.scheduler);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SessionFactoryConfig#scheduler() scheduler} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for scheduler
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSessionFactoryConfig withScheduler(ScheduledExecutorService value) {
    if (this.scheduler == value) return this;
    ScheduledExecutorService newValue = Objects.requireNonNull(value, "scheduler");
    return new ImmutableSessionFactoryConfig(this.clientConfig, this.clientChannelFactory, this.sessionConfig, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableSessionFactoryConfig} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableSessionFactoryConfig
        && equalTo(0, (ImmutableSessionFactoryConfig) another);
  }

  private boolean equalTo(int synthetic, ImmutableSessionFactoryConfig another) {
    return clientConfig.equals(another.clientConfig)
        && clientChannelFactory.equals(another.clientChannelFactory)
        && sessionConfig.equals(another.sessionConfig)
        && scheduler.equals(another.scheduler);
  }

  /**
   * Computes a hash code from attributes: {@code clientConfig}, {@code clientChannelFactory}, {@code sessionConfig}, {@code scheduler}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + clientConfig.hashCode();
    h += (h << 5) + clientChannelFactory.hashCode();
    h += (h << 5) + sessionConfig.hashCode();
    h += (h << 5) + scheduler.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code SessionFactoryConfig} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("SessionFactoryConfig")
        .omitNullValues()
        .add("clientConfig", clientConfig)
        .add("clientChannelFactory", clientChannelFactory)
        .add("sessionConfig", sessionConfig)
        .add("scheduler", scheduler)
        .toString();
  }

  /**
   * Creates an immutable copy of a {@link SessionFactoryConfig} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable SessionFactoryConfig instance
   */
  public static ImmutableSessionFactoryConfig copyOf(SessionFactoryConfig instance) {
    if (instance instanceof ImmutableSessionFactoryConfig) {
      return (ImmutableSessionFactoryConfig) instance;
    }
    return ImmutableSessionFactoryConfig.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableSessionFactoryConfig ImmutableSessionFactoryConfig}.
   * <pre>
   * ImmutableSessionFactoryConfig.builder()
   *    .clientConfig(io.deephaven.client.impl.ClientConfig) // required {@link SessionFactoryConfig#clientConfig() clientConfig}
   *    .clientChannelFactory(io.deephaven.client.impl.ClientChannelFactory) // optional {@link SessionFactoryConfig#clientChannelFactory() clientChannelFactory}
   *    .sessionConfig(io.deephaven.client.impl.SessionConfig) // optional {@link SessionFactoryConfig#sessionConfig() sessionConfig}
   *    .scheduler(concurrent.ScheduledExecutorService) // required {@link SessionFactoryConfig#scheduler() scheduler}
   *    .build();
   * </pre>
   * @return A new ImmutableSessionFactoryConfig builder
   */
  public static ImmutableSessionFactoryConfig.Builder builder() {
    return new ImmutableSessionFactoryConfig.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableSessionFactoryConfig ImmutableSessionFactoryConfig}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "SessionFactoryConfig", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements SessionFactoryConfig.Builder {
    private static final long INIT_BIT_CLIENT_CONFIG = 0x1L;
    private static final long INIT_BIT_SCHEDULER = 0x2L;
    private long initBits = 0x3L;

    private @Nullable ClientConfig clientConfig;
    private @Nullable ClientChannelFactory clientChannelFactory;
    private @Nullable SessionConfig sessionConfig;
    private @Nullable ScheduledExecutorService scheduler;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code SessionFactoryConfig} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(SessionFactoryConfig instance) {
      Objects.requireNonNull(instance, "instance");
      clientConfig(instance.clientConfig());
      clientChannelFactory(instance.clientChannelFactory());
      sessionConfig(instance.sessionConfig());
      scheduler(instance.scheduler());
      return this;
    }

    /**
     * Initializes the value for the {@link SessionFactoryConfig#clientConfig() clientConfig} attribute.
     * @param clientConfig The value for clientConfig 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder clientConfig(ClientConfig clientConfig) {
      this.clientConfig = Objects.requireNonNull(clientConfig, "clientConfig");
      initBits &= ~INIT_BIT_CLIENT_CONFIG;
      return this;
    }

    /**
     * Initializes the value for the {@link SessionFactoryConfig#clientChannelFactory() clientChannelFactory} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link SessionFactoryConfig#clientChannelFactory() clientChannelFactory}.</em>
     * @param clientChannelFactory The value for clientChannelFactory 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder clientChannelFactory(ClientChannelFactory clientChannelFactory) {
      this.clientChannelFactory = Objects.requireNonNull(clientChannelFactory, "clientChannelFactory");
      return this;
    }

    /**
     * Initializes the value for the {@link SessionFactoryConfig#sessionConfig() sessionConfig} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link SessionFactoryConfig#sessionConfig() sessionConfig}.</em>
     * @param sessionConfig The value for sessionConfig 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder sessionConfig(SessionConfig sessionConfig) {
      this.sessionConfig = Objects.requireNonNull(sessionConfig, "sessionConfig");
      return this;
    }

    /**
     * Initializes the value for the {@link SessionFactoryConfig#scheduler() scheduler} attribute.
     * @param scheduler The value for scheduler 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder scheduler(ScheduledExecutorService scheduler) {
      this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
      initBits &= ~INIT_BIT_SCHEDULER;
      return this;
    }

    /**
     * Builds a new {@link ImmutableSessionFactoryConfig ImmutableSessionFactoryConfig}.
     * @return An immutable instance of SessionFactoryConfig
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableSessionFactoryConfig build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableSessionFactoryConfig(this);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_CLIENT_CONFIG) != 0) attributes.add("clientConfig");
      if ((initBits & INIT_BIT_SCHEDULER) != 0) attributes.add("scheduler");
      return "Cannot build SessionFactoryConfig, some of required attributes are not set " + attributes;
    }
  }
}
