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
 * Immutable implementation of {@link RollingMinMaxSpec}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableRollingMinMaxSpec.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableRollingMinMaxSpec.of()}.
 */
@Generated(from = "RollingMinMaxSpec", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableRollingMinMaxSpec extends RollingMinMaxSpec {
<<<<<<< HEAD
  private final WindowScale revWindowScale;
  private final WindowScale fwdWindowScale;
=======
>>>>>>> main
  private final boolean isMax;

  private ImmutableRollingMinMaxSpec(boolean isMax) {
    this.isMax = isMax;
<<<<<<< HEAD
    this.revWindowScale = initShim.revWindowScale();
    this.fwdWindowScale = initShim.fwdWindowScale();
    this.initShim = null;
  }

  private ImmutableRollingMinMaxSpec(ImmutableRollingMinMaxSpec.Builder builder) {
    this.isMax = builder.isMax;
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

  private ImmutableRollingMinMaxSpec(
      WindowScale revWindowScale,
      WindowScale fwdWindowScale,
      boolean isMax) {
    this.revWindowScale = revWindowScale;
    this.fwdWindowScale = fwdWindowScale;
    this.isMax = isMax;
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "RollingMinMaxSpec", generator = "Immutables")
  private final class InitShim {
    private byte revWindowScaleBuildStage = STAGE_UNINITIALIZED;
    private WindowScale revWindowScale;

    WindowScale revWindowScale() {
      if (revWindowScaleBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (revWindowScaleBuildStage == STAGE_UNINITIALIZED) {
        revWindowScaleBuildStage = STAGE_INITIALIZING;
        this.revWindowScale = Objects.requireNonNull(ImmutableRollingMinMaxSpec.super.revWindowScale(), "revWindowScale");
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
        this.fwdWindowScale = Objects.requireNonNull(ImmutableRollingMinMaxSpec.super.fwdWindowScale(), "fwdWindowScale");
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
      return "Cannot build RollingMinMaxSpec, attribute initializers form cycle " + attributes;
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
=======
>>>>>>> main
  }

  /**
   * @return The value of the {@code isMax} attribute
   */
  @Override
  public boolean isMax() {
    return isMax;
  }

  /**
<<<<<<< HEAD
   * Copy the current immutable object by setting a value for the {@link RollingMinMaxSpec#revWindowScale() revWindowScale} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for revWindowScale
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRollingMinMaxSpec withRevWindowScale(WindowScale value) {
    if (this.revWindowScale == value) return this;
    WindowScale newValue = Objects.requireNonNull(value, "revWindowScale");
    return validate(new ImmutableRollingMinMaxSpec(newValue, this.fwdWindowScale, this.isMax));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link RollingMinMaxSpec#fwdWindowScale() fwdWindowScale} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for fwdWindowScale
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRollingMinMaxSpec withFwdWindowScale(WindowScale value) {
    if (this.fwdWindowScale == value) return this;
    WindowScale newValue = Objects.requireNonNull(value, "fwdWindowScale");
    return validate(new ImmutableRollingMinMaxSpec(this.revWindowScale, newValue, this.isMax));
  }

  /**
=======
>>>>>>> main
   * Copy the current immutable object by setting a value for the {@link RollingMinMaxSpec#isMax() isMax} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for isMax
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRollingMinMaxSpec withIsMax(boolean value) {
    if (this.isMax == value) return this;
<<<<<<< HEAD
    return validate(new ImmutableRollingMinMaxSpec(this.revWindowScale, this.fwdWindowScale, value));
=======
    return new ImmutableRollingMinMaxSpec(value);
>>>>>>> main
  }

  /**
   * This instance is equal to all instances of {@code ImmutableRollingMinMaxSpec} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableRollingMinMaxSpec
        && equalTo(0, (ImmutableRollingMinMaxSpec) another);
  }

  private boolean equalTo(int synthetic, ImmutableRollingMinMaxSpec another) {
<<<<<<< HEAD
    return revWindowScale.equals(another.revWindowScale)
        && fwdWindowScale.equals(another.fwdWindowScale)
        && isMax == another.isMax;
  }

  /**
   * Computes a hash code from attributes: {@code revWindowScale}, {@code fwdWindowScale}, {@code isMax}.
=======
    return isMax == another.isMax;
  }

  /**
   * Computes a hash code from attributes: {@code isMax}.
>>>>>>> main
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
<<<<<<< HEAD
    h += (h << 5) + revWindowScale.hashCode();
    h += (h << 5) + fwdWindowScale.hashCode();
=======
>>>>>>> main
    h += (h << 5) + Boolean.hashCode(isMax);
    return h;
  }

  /**
   * Prints the immutable value {@code RollingMinMaxSpec} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "RollingMinMaxSpec{"
<<<<<<< HEAD
        + "revWindowScale=" + revWindowScale
        + ", fwdWindowScale=" + fwdWindowScale
        + ", isMax=" + isMax
=======
        + "isMax=" + isMax
>>>>>>> main
        + "}";
  }

  /**
   * Construct a new immutable {@code RollingMinMaxSpec} instance.
   * @param isMax The value for the {@code isMax} attribute
   * @return An immutable RollingMinMaxSpec instance
   */
  public static ImmutableRollingMinMaxSpec of(boolean isMax) {
<<<<<<< HEAD
    return validate(new ImmutableRollingMinMaxSpec(isMax));
  }

  private static ImmutableRollingMinMaxSpec validate(ImmutableRollingMinMaxSpec instance) {
    instance.checkWindowSizes();
    return instance;
=======
    return new ImmutableRollingMinMaxSpec(isMax);
>>>>>>> main
  }

  /**
   * Creates an immutable copy of a {@link RollingMinMaxSpec} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable RollingMinMaxSpec instance
   */
  public static ImmutableRollingMinMaxSpec copyOf(RollingMinMaxSpec instance) {
    if (instance instanceof ImmutableRollingMinMaxSpec) {
      return (ImmutableRollingMinMaxSpec) instance;
    }
    return ImmutableRollingMinMaxSpec.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableRollingMinMaxSpec ImmutableRollingMinMaxSpec}.
   * <pre>
   * ImmutableRollingMinMaxSpec.builder()
<<<<<<< HEAD
   *    .revWindowScale(io.deephaven.api.updateby.spec.WindowScale) // optional {@link RollingMinMaxSpec#revWindowScale() revWindowScale}
   *    .fwdWindowScale(io.deephaven.api.updateby.spec.WindowScale) // optional {@link RollingMinMaxSpec#fwdWindowScale() fwdWindowScale}
=======
>>>>>>> main
   *    .isMax(boolean) // required {@link RollingMinMaxSpec#isMax() isMax}
   *    .build();
   * </pre>
   * @return A new ImmutableRollingMinMaxSpec builder
   */
  public static ImmutableRollingMinMaxSpec.Builder builder() {
    return new ImmutableRollingMinMaxSpec.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableRollingMinMaxSpec ImmutableRollingMinMaxSpec}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "RollingMinMaxSpec", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_IS_MAX = 0x1L;
    private long initBits = 0x1L;

<<<<<<< HEAD
    private @Nullable WindowScale revWindowScale;
    private @Nullable WindowScale fwdWindowScale;
=======
>>>>>>> main
    private boolean isMax;

    private Builder() {
    }

    /**
<<<<<<< HEAD
     * Fill a builder with attribute values from the provided {@code io.deephaven.api.updateby.spec.RollingMinMaxSpec} instance.
=======
     * Fill a builder with attribute values from the provided {@code RollingMinMaxSpec} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
>>>>>>> main
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(RollingMinMaxSpec instance) {
      Objects.requireNonNull(instance, "instance");
<<<<<<< HEAD
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
      if (object instanceof RollingMinMaxSpec) {
        RollingMinMaxSpec instance = (RollingMinMaxSpec) object;
        if ((bits & 0x2L) == 0) {
          revWindowScale(instance.revWindowScale());
          bits |= 0x2L;
        }
        if ((bits & 0x1L) == 0) {
          fwdWindowScale(instance.fwdWindowScale());
          bits |= 0x1L;
        }
        isMax(instance.isMax());
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
     * Initializes the value for the {@link RollingMinMaxSpec#revWindowScale() revWindowScale} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link RollingMinMaxSpec#revWindowScale() revWindowScale}.</em>
     * @param revWindowScale The value for revWindowScale 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder revWindowScale(WindowScale revWindowScale) {
      this.revWindowScale = Objects.requireNonNull(revWindowScale, "revWindowScale");
      return this;
    }

    /**
     * Initializes the value for the {@link RollingMinMaxSpec#fwdWindowScale() fwdWindowScale} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link RollingMinMaxSpec#fwdWindowScale() fwdWindowScale}.</em>
     * @param fwdWindowScale The value for fwdWindowScale 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder fwdWindowScale(WindowScale fwdWindowScale) {
      this.fwdWindowScale = Objects.requireNonNull(fwdWindowScale, "fwdWindowScale");
=======
      isMax(instance.isMax());
>>>>>>> main
      return this;
    }

    /**
     * Initializes the value for the {@link RollingMinMaxSpec#isMax() isMax} attribute.
     * @param isMax The value for isMax 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder isMax(boolean isMax) {
      this.isMax = isMax;
      initBits &= ~INIT_BIT_IS_MAX;
      return this;
    }

    /**
     * Builds a new {@link ImmutableRollingMinMaxSpec ImmutableRollingMinMaxSpec}.
     * @return An immutable instance of RollingMinMaxSpec
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableRollingMinMaxSpec build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
<<<<<<< HEAD
      return ImmutableRollingMinMaxSpec.validate(new ImmutableRollingMinMaxSpec(this));
=======
      return new ImmutableRollingMinMaxSpec(isMax);
>>>>>>> main
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_IS_MAX) != 0) attributes.add("isMax");
      return "Cannot build RollingMinMaxSpec, some of required attributes are not set " + attributes;
    }
  }
}
