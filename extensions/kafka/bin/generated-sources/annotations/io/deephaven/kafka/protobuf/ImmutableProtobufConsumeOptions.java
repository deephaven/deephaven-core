package io.deephaven.kafka.protobuf;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import io.deephaven.protobuf.ProtobufDescriptorParserOptions;
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
 * Immutable implementation of {@link ProtobufConsumeOptions}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableProtobufConsumeOptions.builder()}.
 */
@Generated(from = "ProtobufConsumeOptions", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableProtobufConsumeOptions extends ProtobufConsumeOptions {
  private final DescriptorProvider descriptorProvider;
  private final Protocol protocol;
  private final ProtobufDescriptorParserOptions parserOptions;
  private final ProtobufConsumeOptions.FieldPathToColumnName pathToColumnName;

  private ImmutableProtobufConsumeOptions(ImmutableProtobufConsumeOptions.Builder builder) {
    this.descriptorProvider = builder.descriptorProvider;
    if (builder.protocol != null) {
      initShim.protocol(builder.protocol);
    }
    if (builder.parserOptions != null) {
      initShim.parserOptions(builder.parserOptions);
    }
    if (builder.pathToColumnName != null) {
      initShim.pathToColumnName(builder.pathToColumnName);
    }
    this.protocol = initShim.protocol();
    this.parserOptions = initShim.parserOptions();
    this.pathToColumnName = initShim.pathToColumnName();
    this.initShim = null;
  }

  private ImmutableProtobufConsumeOptions(
      DescriptorProvider descriptorProvider,
      Protocol protocol,
      ProtobufDescriptorParserOptions parserOptions,
      ProtobufConsumeOptions.FieldPathToColumnName pathToColumnName) {
    this.descriptorProvider = descriptorProvider;
    this.protocol = protocol;
    this.parserOptions = parserOptions;
    this.pathToColumnName = pathToColumnName;
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  @SuppressWarnings("Immutable")
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "ProtobufConsumeOptions", generator = "Immutables")
  private final class InitShim {
    private byte protocolBuildStage = STAGE_UNINITIALIZED;
    private Protocol protocol;

    Protocol protocol() {
      if (protocolBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (protocolBuildStage == STAGE_UNINITIALIZED) {
        protocolBuildStage = STAGE_INITIALIZING;
        this.protocol = Objects.requireNonNull(ImmutableProtobufConsumeOptions.super.protocol(), "protocol");
        protocolBuildStage = STAGE_INITIALIZED;
      }
      return this.protocol;
    }

    void protocol(Protocol protocol) {
      this.protocol = protocol;
      protocolBuildStage = STAGE_INITIALIZED;
    }

    private byte parserOptionsBuildStage = STAGE_UNINITIALIZED;
    private ProtobufDescriptorParserOptions parserOptions;

    ProtobufDescriptorParserOptions parserOptions() {
      if (parserOptionsBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (parserOptionsBuildStage == STAGE_UNINITIALIZED) {
        parserOptionsBuildStage = STAGE_INITIALIZING;
        this.parserOptions = Objects.requireNonNull(ImmutableProtobufConsumeOptions.super.parserOptions(), "parserOptions");
        parserOptionsBuildStage = STAGE_INITIALIZED;
      }
      return this.parserOptions;
    }

    void parserOptions(ProtobufDescriptorParserOptions parserOptions) {
      this.parserOptions = parserOptions;
      parserOptionsBuildStage = STAGE_INITIALIZED;
    }

    private byte pathToColumnNameBuildStage = STAGE_UNINITIALIZED;
    private ProtobufConsumeOptions.FieldPathToColumnName pathToColumnName;

    ProtobufConsumeOptions.FieldPathToColumnName pathToColumnName() {
      if (pathToColumnNameBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (pathToColumnNameBuildStage == STAGE_UNINITIALIZED) {
        pathToColumnNameBuildStage = STAGE_INITIALIZING;
        this.pathToColumnName = Objects.requireNonNull(ImmutableProtobufConsumeOptions.super.pathToColumnName(), "pathToColumnName");
        pathToColumnNameBuildStage = STAGE_INITIALIZED;
      }
      return this.pathToColumnName;
    }

    void pathToColumnName(ProtobufConsumeOptions.FieldPathToColumnName pathToColumnName) {
      this.pathToColumnName = pathToColumnName;
      pathToColumnNameBuildStage = STAGE_INITIALIZED;
    }

    private String formatInitCycleMessage() {
      List<String> attributes = new ArrayList<>();
      if (protocolBuildStage == STAGE_INITIALIZING) attributes.add("protocol");
      if (parserOptionsBuildStage == STAGE_INITIALIZING) attributes.add("parserOptions");
      if (pathToColumnNameBuildStage == STAGE_INITIALIZING) attributes.add("pathToColumnName");
      return "Cannot build ProtobufConsumeOptions, attribute initializers form cycle " + attributes;
    }
  }

  /**
   * The descriptor provider.
   * @return the descriptor provider
   */
  @Override
  public DescriptorProvider descriptorProvider() {
    return descriptorProvider;
  }

  /**
   * The protocol for decoding the payload. When {@link #descriptorProvider()} is a {@link DescriptorSchemaRegistry},
   * {@link Protocol#serdes()} will be used by default; when {@link #descriptorProvider()} is a
   * {@link DescriptorMessageClass}, {@link Protocol#raw()} will be used by default.
   * @return the payload protocol
   */
  @Override
  public Protocol protocol() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.protocol()
        : this.protocol;
  }

  /**
   * The descriptor parsing options. By default, is {@link ProtobufDescriptorParserOptions#defaults()}.
   * @return the descriptor parsing options
   */
  @Override
  public ProtobufDescriptorParserOptions parserOptions() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.parserOptions()
        : this.parserOptions;
  }

  /**
   * The function to turn field paths into column names. By default, is the function
   * {@link #joinNamePathWithUnderscore(FieldPath, int)}}.
   * @return the function to create column names
   */
  @Override
  public ProtobufConsumeOptions.FieldPathToColumnName pathToColumnName() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.pathToColumnName()
        : this.pathToColumnName;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ProtobufConsumeOptions#descriptorProvider() descriptorProvider} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for descriptorProvider
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableProtobufConsumeOptions withDescriptorProvider(DescriptorProvider value) {
    if (this.descriptorProvider == value) return this;
    DescriptorProvider newValue = Objects.requireNonNull(value, "descriptorProvider");
    return new ImmutableProtobufConsumeOptions(newValue, this.protocol, this.parserOptions, this.pathToColumnName);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ProtobufConsumeOptions#protocol() protocol} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for protocol
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableProtobufConsumeOptions withProtocol(Protocol value) {
    if (this.protocol == value) return this;
    Protocol newValue = Objects.requireNonNull(value, "protocol");
    return new ImmutableProtobufConsumeOptions(this.descriptorProvider, newValue, this.parserOptions, this.pathToColumnName);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ProtobufConsumeOptions#parserOptions() parserOptions} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for parserOptions
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableProtobufConsumeOptions withParserOptions(ProtobufDescriptorParserOptions value) {
    if (this.parserOptions == value) return this;
    ProtobufDescriptorParserOptions newValue = Objects.requireNonNull(value, "parserOptions");
    return new ImmutableProtobufConsumeOptions(this.descriptorProvider, this.protocol, newValue, this.pathToColumnName);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ProtobufConsumeOptions#pathToColumnName() pathToColumnName} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for pathToColumnName
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableProtobufConsumeOptions withPathToColumnName(ProtobufConsumeOptions.FieldPathToColumnName value) {
    if (this.pathToColumnName == value) return this;
    ProtobufConsumeOptions.FieldPathToColumnName newValue = Objects.requireNonNull(value, "pathToColumnName");
    return new ImmutableProtobufConsumeOptions(this.descriptorProvider, this.protocol, this.parserOptions, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableProtobufConsumeOptions} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableProtobufConsumeOptions
        && equalTo(0, (ImmutableProtobufConsumeOptions) another);
  }

  private boolean equalTo(int synthetic, ImmutableProtobufConsumeOptions another) {
    return descriptorProvider.equals(another.descriptorProvider)
        && protocol.equals(another.protocol)
        && parserOptions.equals(another.parserOptions)
        && pathToColumnName.equals(another.pathToColumnName);
  }

  /**
   * Computes a hash code from attributes: {@code descriptorProvider}, {@code protocol}, {@code parserOptions}, {@code pathToColumnName}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + descriptorProvider.hashCode();
    h += (h << 5) + protocol.hashCode();
    h += (h << 5) + parserOptions.hashCode();
    h += (h << 5) + pathToColumnName.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code ProtobufConsumeOptions} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("ProtobufConsumeOptions")
        .omitNullValues()
        .add("descriptorProvider", descriptorProvider)
        .add("protocol", protocol)
        .add("parserOptions", parserOptions)
        .add("pathToColumnName", pathToColumnName)
        .toString();
  }

  /**
   * Creates an immutable copy of a {@link ProtobufConsumeOptions} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ProtobufConsumeOptions instance
   */
  public static ImmutableProtobufConsumeOptions copyOf(ProtobufConsumeOptions instance) {
    if (instance instanceof ImmutableProtobufConsumeOptions) {
      return (ImmutableProtobufConsumeOptions) instance;
    }
    return ImmutableProtobufConsumeOptions.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableProtobufConsumeOptions ImmutableProtobufConsumeOptions}.
   * <pre>
   * ImmutableProtobufConsumeOptions.builder()
   *    .descriptorProvider(io.deephaven.kafka.protobuf.DescriptorProvider) // required {@link ProtobufConsumeOptions#descriptorProvider() descriptorProvider}
   *    .protocol(io.deephaven.kafka.protobuf.Protocol) // optional {@link ProtobufConsumeOptions#protocol() protocol}
   *    .parserOptions(io.deephaven.protobuf.ProtobufDescriptorParserOptions) // optional {@link ProtobufConsumeOptions#parserOptions() parserOptions}
   *    .pathToColumnName(io.deephaven.kafka.protobuf.ProtobufConsumeOptions.FieldPathToColumnName) // optional {@link ProtobufConsumeOptions#pathToColumnName() pathToColumnName}
   *    .build();
   * </pre>
   * @return A new ImmutableProtobufConsumeOptions builder
   */
  public static ImmutableProtobufConsumeOptions.Builder builder() {
    return new ImmutableProtobufConsumeOptions.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableProtobufConsumeOptions ImmutableProtobufConsumeOptions}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ProtobufConsumeOptions", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements ProtobufConsumeOptions.Builder {
    private static final long INIT_BIT_DESCRIPTOR_PROVIDER = 0x1L;
    private long initBits = 0x1L;

    private @Nullable DescriptorProvider descriptorProvider;
    private @Nullable Protocol protocol;
    private @Nullable ProtobufDescriptorParserOptions parserOptions;
    private @Nullable ProtobufConsumeOptions.FieldPathToColumnName pathToColumnName;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ProtobufConsumeOptions} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(ProtobufConsumeOptions instance) {
      Objects.requireNonNull(instance, "instance");
      descriptorProvider(instance.descriptorProvider());
      protocol(instance.protocol());
      parserOptions(instance.parserOptions());
      pathToColumnName(instance.pathToColumnName());
      return this;
    }

    /**
     * Initializes the value for the {@link ProtobufConsumeOptions#descriptorProvider() descriptorProvider} attribute.
     * @param descriptorProvider The value for descriptorProvider 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder descriptorProvider(DescriptorProvider descriptorProvider) {
      this.descriptorProvider = Objects.requireNonNull(descriptorProvider, "descriptorProvider");
      initBits &= ~INIT_BIT_DESCRIPTOR_PROVIDER;
      return this;
    }

    /**
     * Initializes the value for the {@link ProtobufConsumeOptions#protocol() protocol} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link ProtobufConsumeOptions#protocol() protocol}.</em>
     * @param protocol The value for protocol 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder protocol(Protocol protocol) {
      this.protocol = Objects.requireNonNull(protocol, "protocol");
      return this;
    }

    /**
     * Initializes the value for the {@link ProtobufConsumeOptions#parserOptions() parserOptions} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link ProtobufConsumeOptions#parserOptions() parserOptions}.</em>
     * @param parserOptions The value for parserOptions 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder parserOptions(ProtobufDescriptorParserOptions parserOptions) {
      this.parserOptions = Objects.requireNonNull(parserOptions, "parserOptions");
      return this;
    }

    /**
     * Initializes the value for the {@link ProtobufConsumeOptions#pathToColumnName() pathToColumnName} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link ProtobufConsumeOptions#pathToColumnName() pathToColumnName}.</em>
     * @param pathToColumnName The value for pathToColumnName 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder pathToColumnName(ProtobufConsumeOptions.FieldPathToColumnName pathToColumnName) {
      this.pathToColumnName = Objects.requireNonNull(pathToColumnName, "pathToColumnName");
      return this;
    }

    /**
     * Builds a new {@link ImmutableProtobufConsumeOptions ImmutableProtobufConsumeOptions}.
     * @return An immutable instance of ProtobufConsumeOptions
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableProtobufConsumeOptions build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableProtobufConsumeOptions(this);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_DESCRIPTOR_PROVIDER) != 0) attributes.add("descriptorProvider");
      return "Cannot build ProtobufConsumeOptions, some of required attributes are not set " + attributes;
    }
  }
}
