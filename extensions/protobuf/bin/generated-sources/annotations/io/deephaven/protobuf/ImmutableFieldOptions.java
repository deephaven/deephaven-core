package io.deephaven.protobuf;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link FieldOptions}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableFieldOptions.builder()}.
 */
@Generated(from = "FieldOptions", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableFieldOptions extends FieldOptions {
  private final boolean include;
  private final FieldOptions.WellKnownBehavior wellKnown;
  private final FieldOptions.BytesBehavior bytes;
  private final FieldOptions.MapBehavior map;

  private ImmutableFieldOptions(ImmutableFieldOptions.Builder builder) {
    if (builder.includeIsSet()) {
      initShim.include(builder.include);
    }
    if (builder.wellKnown != null) {
      initShim.wellKnown(builder.wellKnown);
    }
    if (builder.bytes != null) {
      initShim.bytes(builder.bytes);
    }
    if (builder.map != null) {
      initShim.map(builder.map);
    }
    this.include = initShim.include();
    this.wellKnown = initShim.wellKnown();
    this.bytes = initShim.bytes();
    this.map = initShim.map();
    this.initShim = null;
  }

  private ImmutableFieldOptions(
      boolean include,
      FieldOptions.WellKnownBehavior wellKnown,
      FieldOptions.BytesBehavior bytes,
      FieldOptions.MapBehavior map) {
    this.include = include;
    this.wellKnown = wellKnown;
    this.bytes = bytes;
    this.map = map;
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "FieldOptions", generator = "Immutables")
  private final class InitShim {
    private byte includeBuildStage = STAGE_UNINITIALIZED;
    private boolean include;

    boolean include() {
      if (includeBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (includeBuildStage == STAGE_UNINITIALIZED) {
        includeBuildStage = STAGE_INITIALIZING;
        this.include = ImmutableFieldOptions.super.include();
        includeBuildStage = STAGE_INITIALIZED;
      }
      return this.include;
    }

    void include(boolean include) {
      this.include = include;
      includeBuildStage = STAGE_INITIALIZED;
    }

    private byte wellKnownBuildStage = STAGE_UNINITIALIZED;
    private FieldOptions.WellKnownBehavior wellKnown;

    FieldOptions.WellKnownBehavior wellKnown() {
      if (wellKnownBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (wellKnownBuildStage == STAGE_UNINITIALIZED) {
        wellKnownBuildStage = STAGE_INITIALIZING;
        this.wellKnown = Objects.requireNonNull(ImmutableFieldOptions.super.wellKnown(), "wellKnown");
        wellKnownBuildStage = STAGE_INITIALIZED;
      }
      return this.wellKnown;
    }

    void wellKnown(FieldOptions.WellKnownBehavior wellKnown) {
      this.wellKnown = wellKnown;
      wellKnownBuildStage = STAGE_INITIALIZED;
    }

    private byte bytesBuildStage = STAGE_UNINITIALIZED;
    private FieldOptions.BytesBehavior bytes;

    FieldOptions.BytesBehavior bytes() {
      if (bytesBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (bytesBuildStage == STAGE_UNINITIALIZED) {
        bytesBuildStage = STAGE_INITIALIZING;
        this.bytes = Objects.requireNonNull(ImmutableFieldOptions.super.bytes(), "bytes");
        bytesBuildStage = STAGE_INITIALIZED;
      }
      return this.bytes;
    }

    void bytes(FieldOptions.BytesBehavior bytes) {
      this.bytes = bytes;
      bytesBuildStage = STAGE_INITIALIZED;
    }

    private byte mapBuildStage = STAGE_UNINITIALIZED;
    private FieldOptions.MapBehavior map;

    FieldOptions.MapBehavior map() {
      if (mapBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (mapBuildStage == STAGE_UNINITIALIZED) {
        mapBuildStage = STAGE_INITIALIZING;
        this.map = Objects.requireNonNull(ImmutableFieldOptions.super.map(), "map");
        mapBuildStage = STAGE_INITIALIZED;
      }
      return this.map;
    }

    void map(FieldOptions.MapBehavior map) {
      this.map = map;
      mapBuildStage = STAGE_INITIALIZED;
    }

    private String formatInitCycleMessage() {
      List<String> attributes = new ArrayList<>();
      if (includeBuildStage == STAGE_INITIALIZING) attributes.add("include");
      if (wellKnownBuildStage == STAGE_INITIALIZING) attributes.add("wellKnown");
      if (bytesBuildStage == STAGE_INITIALIZING) attributes.add("bytes");
      if (mapBuildStage == STAGE_INITIALIZING) attributes.add("map");
      return "Cannot build FieldOptions, attribute initializers form cycle " + attributes;
    }
  }

  /**
   * If the field should be included for parsing. By default, is {@code true}.
   * @return if the field should be included
   */
  @Override
  public boolean include() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.include()
        : this.include;
  }

  /**
   * The well-known message behavior. By default, is {@link WellKnownBehavior#asWellKnown()}.
   * @return the well-known message behavior
   */
  @Override
  public FieldOptions.WellKnownBehavior wellKnown() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.wellKnown()
        : this.wellKnown;
  }

  /**
   * The {@code bytes} type behavior. By default, is {@link BytesBehavior#asByteArray()}.
   * @return the bytes field behavior
   */
  @Override
  public FieldOptions.BytesBehavior bytes() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.bytes()
        : this.bytes;
  }

  /**
   * The {@code map} type behavior. By default, is {@link MapBehavior#asMap()}.
   * @return the map field behavior.
   */
  @Override
  public FieldOptions.MapBehavior map() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.map()
        : this.map;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link FieldOptions#include() include} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for include
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableFieldOptions withInclude(boolean value) {
    if (this.include == value) return this;
    return new ImmutableFieldOptions(value, this.wellKnown, this.bytes, this.map);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link FieldOptions#wellKnown() wellKnown} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for wellKnown
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableFieldOptions withWellKnown(FieldOptions.WellKnownBehavior value) {
    if (this.wellKnown == value) return this;
    FieldOptions.WellKnownBehavior newValue = Objects.requireNonNull(value, "wellKnown");
    return new ImmutableFieldOptions(this.include, newValue, this.bytes, this.map);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link FieldOptions#bytes() bytes} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for bytes
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableFieldOptions withBytes(FieldOptions.BytesBehavior value) {
    if (this.bytes == value) return this;
    FieldOptions.BytesBehavior newValue = Objects.requireNonNull(value, "bytes");
    return new ImmutableFieldOptions(this.include, this.wellKnown, newValue, this.map);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link FieldOptions#map() map} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for map
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableFieldOptions withMap(FieldOptions.MapBehavior value) {
    if (this.map == value) return this;
    FieldOptions.MapBehavior newValue = Objects.requireNonNull(value, "map");
    return new ImmutableFieldOptions(this.include, this.wellKnown, this.bytes, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableFieldOptions} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableFieldOptions
        && equalTo(0, (ImmutableFieldOptions) another);
  }

  private boolean equalTo(int synthetic, ImmutableFieldOptions another) {
    return include == another.include
        && wellKnown.equals(another.wellKnown)
        && bytes.equals(another.bytes)
        && map.equals(another.map);
  }

  /**
   * Computes a hash code from attributes: {@code include}, {@code wellKnown}, {@code bytes}, {@code map}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Boolean.hashCode(include);
    h += (h << 5) + wellKnown.hashCode();
    h += (h << 5) + bytes.hashCode();
    h += (h << 5) + map.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code FieldOptions} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "FieldOptions{"
        + "include=" + include
        + ", wellKnown=" + wellKnown
        + ", bytes=" + bytes
        + ", map=" + map
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link FieldOptions} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable FieldOptions instance
   */
  public static ImmutableFieldOptions copyOf(FieldOptions instance) {
    if (instance instanceof ImmutableFieldOptions) {
      return (ImmutableFieldOptions) instance;
    }
    return ImmutableFieldOptions.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableFieldOptions ImmutableFieldOptions}.
   * <pre>
   * ImmutableFieldOptions.builder()
   *    .include(boolean) // optional {@link FieldOptions#include() include}
   *    .wellKnown(io.deephaven.protobuf.FieldOptions.WellKnownBehavior) // optional {@link FieldOptions#wellKnown() wellKnown}
   *    .bytes(io.deephaven.protobuf.FieldOptions.BytesBehavior) // optional {@link FieldOptions#bytes() bytes}
   *    .map(io.deephaven.protobuf.FieldOptions.MapBehavior) // optional {@link FieldOptions#map() map}
   *    .build();
   * </pre>
   * @return A new ImmutableFieldOptions builder
   */
  public static ImmutableFieldOptions.Builder builder() {
    return new ImmutableFieldOptions.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableFieldOptions ImmutableFieldOptions}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "FieldOptions", generator = "Immutables")
  public static final class Builder implements FieldOptions.Builder {
    private static final long OPT_BIT_INCLUDE = 0x1L;
    private long optBits;

    private boolean include;
    private FieldOptions.WellKnownBehavior wellKnown;
    private FieldOptions.BytesBehavior bytes;
    private FieldOptions.MapBehavior map;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code FieldOptions} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(FieldOptions instance) {
      Objects.requireNonNull(instance, "instance");
      include(instance.include());
      wellKnown(instance.wellKnown());
      bytes(instance.bytes());
      map(instance.map());
      return this;
    }

    /**
     * Initializes the value for the {@link FieldOptions#include() include} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link FieldOptions#include() include}.</em>
     * @param include The value for include 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder include(boolean include) {
      this.include = include;
      optBits |= OPT_BIT_INCLUDE;
      return this;
    }

    /**
     * Initializes the value for the {@link FieldOptions#wellKnown() wellKnown} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link FieldOptions#wellKnown() wellKnown}.</em>
     * @param wellKnown The value for wellKnown 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder wellKnown(FieldOptions.WellKnownBehavior wellKnown) {
      this.wellKnown = Objects.requireNonNull(wellKnown, "wellKnown");
      return this;
    }

    /**
     * Initializes the value for the {@link FieldOptions#bytes() bytes} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link FieldOptions#bytes() bytes}.</em>
     * @param bytes The value for bytes 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder bytes(FieldOptions.BytesBehavior bytes) {
      this.bytes = Objects.requireNonNull(bytes, "bytes");
      return this;
    }

    /**
     * Initializes the value for the {@link FieldOptions#map() map} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link FieldOptions#map() map}.</em>
     * @param map The value for map 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder map(FieldOptions.MapBehavior map) {
      this.map = Objects.requireNonNull(map, "map");
      return this;
    }

    /**
     * Builds a new {@link ImmutableFieldOptions ImmutableFieldOptions}.
     * @return An immutable instance of FieldOptions
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableFieldOptions build() {
      return new ImmutableFieldOptions(this);
    }

    private boolean includeIsSet() {
      return (optBits & OPT_BIT_INCLUDE) != 0;
    }
  }
}
