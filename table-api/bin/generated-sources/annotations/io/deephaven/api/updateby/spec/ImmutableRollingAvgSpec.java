package io.deephaven.api.updateby.spec;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link RollingAvgSpec}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableRollingAvgSpec.builder()}.
 */
@Generated(from = "RollingAvgSpec", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableRollingAvgSpec extends RollingAvgSpec {
  private final WindowScale revWindowScale;
  private final WindowScale fwdWindowScale;

  private ImmutableRollingAvgSpec(ImmutableRollingAvgSpec.Builder builder) {
    if (builder.revWindowScale != null) {
      initShim.revWindowScale(builder.revWindowScale);
    }
    if (builder.fwdWindowScale != null) {
      initShim.fwdWindowScale(builder.fwdWindowScale);
    }
    this.revWindowScale = initShim.revWindowScale();
    this.fwdWindowScale = initShim.fwdWindowScale();
    this.initShim = null;
  }

  private ImmutableRollingAvgSpec(
      WindowScale revWindowScale,
      WindowScale fwdWindowScale) {
    this.revWindowScale = revWindowScale;
    this.fwdWindowScale = fwdWindowScale;
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "RollingAvgSpec", generator = "Immutables")
  private final class InitShim {
    private byte revWindowScaleBuildStage = STAGE_UNINITIALIZED;
    private WindowScale revWindowScale;

    WindowScale revWindowScale() {
      if (revWindowScaleBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (revWindowScaleBuildStage == STAGE_UNINITIALIZED) {
        revWindowScaleBuildStage = STAGE_INITIALIZING;
        this.revWindowScale = Objects.requireNonNull(ImmutableRollingAvgSpec.super.revWindowScale(), "revWindowScale");
        revWindowScaleBuildStage = STAGE_INITIALIZED;
      }
      return this.revWindowScale;
    }

    void revWindowScale(WindowScale revWindowScale) {
      this.revWindowScale = revWindowScale;
      revWindowScaleBuildStage = STAGE_INITIALIZED;
    }

    private byte fwdWindowScaleBuildStage = STAGE_UNINITIALIZED;
    private WindowScale fwdWindowScale;

    WindowScale fwdWindowScale() {
      if (fwdWindowScaleBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (fwdWindowScaleBuildStage == STAGE_UNINITIALIZED) {
        fwdWindowScaleBuildStage = STAGE_INITIALIZING;
        this.fwdWindowScale = Objects.requireNonNull(ImmutableRollingAvgSpec.super.fwdWindowScale(), "fwdWindowScale");
        fwdWindowScaleBuildStage = STAGE_INITIALIZED;
      }
      return this.fwdWindowScale;
    }

    void fwdWindowScale(WindowScale fwdWindowScale) {
      this.fwdWindowScale = fwdWindowScale;
      fwdWindowScaleBuildStage = STAGE_INITIALIZED;
    }

    private String formatInitCycleMessage() {
      List<String> attributes = new ArrayList<>();
      if (revWindowScaleBuildStage == STAGE_INITIALIZING) attributes.add("revWindowScale");
      if (fwdWindowScaleBuildStage == STAGE_INITIALIZING) attributes.add("fwdWindowScale");
      return "Cannot build RollingAvgSpec, attribute initializers form cycle " + attributes;
    }
  }

  /**
   * @return The value of the {@code revWindowScale} attribute
   */
  @Override
  public WindowScale revWindowScale() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.revWindowScale()
        : this.revWindowScale;
  }

  /**
   * @return The value of the {@code fwdWindowScale} attribute
   */
  @Override
  public WindowScale fwdWindowScale() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.fwdWindowScale()
        : this.fwdWindowScale;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link RollingAvgSpec#revWindowScale() revWindowScale} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for revWindowScale
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRollingAvgSpec withRevWindowScale(WindowScale value) {
    if (this.revWindowScale == value) return this;
    WindowScale newValue = Objects.requireNonNull(value, "revWindowScale");
    return validate(new ImmutableRollingAvgSpec(newValue, this.fwdWindowScale));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link RollingAvgSpec#fwdWindowScale() fwdWindowScale} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for fwdWindowScale
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRollingAvgSpec withFwdWindowScale(WindowScale value) {
    if (this.fwdWindowScale == value) return this;
    WindowScale newValue = Objects.requireNonNull(value, "fwdWindowScale");
    return validate(new ImmutableRollingAvgSpec(this.revWindowScale, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableRollingAvgSpec} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableRollingAvgSpec
        && equalTo(0, (ImmutableRollingAvgSpec) another);
  }

  private boolean equalTo(int synthetic, ImmutableRollingAvgSpec another) {
    return revWindowScale.equals(another.revWindowScale)
        && fwdWindowScale.equals(another.fwdWindowScale);
  }

  /**
   * Computes a hash code from attributes: {@code revWindowScale}, {@code fwdWindowScale}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + revWindowScale.hashCode();
    h += (h << 5) + fwdWindowScale.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code RollingAvgSpec} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "RollingAvgSpec{"
        + "revWindowScale=" + revWindowScale
        + ", fwdWindowScale=" + fwdWindowScale
        + "}";
  }

  private static ImmutableRollingAvgSpec validate(ImmutableRollingAvgSpec instance) {
    instance.checkWindowSizes();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link RollingAvgSpec} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable RollingAvgSpec instance
   */
  public static ImmutableRollingAvgSpec copyOf(RollingAvgSpec instance) {
    if (instance instanceof ImmutableRollingAvgSpec) {
      return (ImmutableRollingAvgSpec) instance;
    }
    return ImmutableRollingAvgSpec.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableRollingAvgSpec ImmutableRollingAvgSpec}.
   * <pre>
   * ImmutableRollingAvgSpec.builder()
   *    .revWindowScale(io.deephaven.api.updateby.spec.WindowScale) // optional {@link RollingAvgSpec#revWindowScale() revWindowScale}
   *    .fwdWindowScale(io.deephaven.api.updateby.spec.WindowScale) // optional {@link RollingAvgSpec#fwdWindowScale() fwdWindowScale}
   *    .build();
   * </pre>
   * @return A new ImmutableRollingAvgSpec builder
   */
  public static ImmutableRollingAvgSpec.Builder builder() {
    return new ImmutableRollingAvgSpec.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableRollingAvgSpec ImmutableRollingAvgSpec}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "RollingAvgSpec", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private @Nullable WindowScale revWindowScale;
    private @Nullable WindowScale fwdWindowScale;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.api.updateby.spec.RollingAvgSpec} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(RollingAvgSpec instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.api.updateby.spec.RollingOpSpec} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(RollingOpSpec instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    private void from(Object object) {
      long bits = 0;
      if (object instanceof RollingAvgSpec) {
        RollingAvgSpec instance = (RollingAvgSpec) object;
        if ((bits & 0x2L) == 0) {
          revWindowScale(instance.revWindowScale());
          bits |= 0x2L;
        }
        if ((bits & 0x1L) == 0) {
          fwdWindowScale(instance.fwdWindowScale());
          bits |= 0x1L;
        }
      }
      if (object instanceof RollingOpSpec) {
        RollingOpSpec instance = (RollingOpSpec) object;
        if ((bits & 0x2L) == 0) {
          revWindowScale(instance.revWindowScale());
          bits |= 0x2L;
        }
        if ((bits & 0x1L) == 0) {
          fwdWindowScale(instance.fwdWindowScale());
          bits |= 0x1L;
        }
      }
    }

    /**
     * Initializes the value for the {@link RollingAvgSpec#revWindowScale() revWindowScale} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link RollingAvgSpec#revWindowScale() revWindowScale}.</em>
     * @param revWindowScale The value for revWindowScale 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder revWindowScale(WindowScale revWindowScale) {
      this.revWindowScale = Objects.requireNonNull(revWindowScale, "revWindowScale");
      return this;
    }

    /**
     * Initializes the value for the {@link RollingAvgSpec#fwdWindowScale() fwdWindowScale} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link RollingAvgSpec#fwdWindowScale() fwdWindowScale}.</em>
     * @param fwdWindowScale The value for fwdWindowScale 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder fwdWindowScale(WindowScale fwdWindowScale) {
      this.fwdWindowScale = Objects.requireNonNull(fwdWindowScale, "fwdWindowScale");
      return this;
    }

    /**
     * Builds a new {@link ImmutableRollingAvgSpec ImmutableRollingAvgSpec}.
     * @return An immutable instance of RollingAvgSpec
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableRollingAvgSpec build() {
      return ImmutableRollingAvgSpec.validate(new ImmutableRollingAvgSpec(this));
    }
  }
}
