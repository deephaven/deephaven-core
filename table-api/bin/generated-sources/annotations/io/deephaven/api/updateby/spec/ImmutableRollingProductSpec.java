package io.deephaven.api.updateby.spec;

<<<<<<< HEAD
import java.util.ArrayList;
import java.util.List;
=======
>>>>>>> main
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link RollingProductSpec}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableRollingProductSpec.builder()}.
 */
@Generated(from = "RollingProductSpec", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableRollingProductSpec extends RollingProductSpec {
<<<<<<< HEAD
  private final WindowScale revWindowScale;
  private final WindowScale fwdWindowScale;

  private ImmutableRollingProductSpec(ImmutableRollingProductSpec.Builder builder) {
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

  private ImmutableRollingProductSpec(
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

  @Generated(from = "RollingProductSpec", generator = "Immutables")
  private final class InitShim {
    private byte revWindowScaleBuildStage = STAGE_UNINITIALIZED;
    private WindowScale revWindowScale;

    WindowScale revWindowScale() {
      if (revWindowScaleBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (revWindowScaleBuildStage == STAGE_UNINITIALIZED) {
        revWindowScaleBuildStage = STAGE_INITIALIZING;
        this.revWindowScale = Objects.requireNonNull(ImmutableRollingProductSpec.super.revWindowScale(), "revWindowScale");
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
        this.fwdWindowScale = Objects.requireNonNull(ImmutableRollingProductSpec.super.fwdWindowScale(), "fwdWindowScale");
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
      return "Cannot build RollingProductSpec, attribute initializers form cycle " + attributes;
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
   * Copy the current immutable object by setting a value for the {@link RollingProductSpec#revWindowScale() revWindowScale} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for revWindowScale
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRollingProductSpec withRevWindowScale(WindowScale value) {
    if (this.revWindowScale == value) return this;
    WindowScale newValue = Objects.requireNonNull(value, "revWindowScale");
    return validate(new ImmutableRollingProductSpec(newValue, this.fwdWindowScale));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link RollingProductSpec#fwdWindowScale() fwdWindowScale} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for fwdWindowScale
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRollingProductSpec withFwdWindowScale(WindowScale value) {
    if (this.fwdWindowScale == value) return this;
    WindowScale newValue = Objects.requireNonNull(value, "fwdWindowScale");
    return validate(new ImmutableRollingProductSpec(this.revWindowScale, newValue));
=======

  private ImmutableRollingProductSpec(ImmutableRollingProductSpec.Builder builder) {
>>>>>>> main
  }

  /**
   * This instance is equal to all instances of {@code ImmutableRollingProductSpec} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableRollingProductSpec
        && equalTo(0, (ImmutableRollingProductSpec) another);
  }

<<<<<<< HEAD
  private boolean equalTo(int synthetic, ImmutableRollingProductSpec another) {
    return revWindowScale.equals(another.revWindowScale)
        && fwdWindowScale.equals(another.fwdWindowScale);
  }

  /**
   * Computes a hash code from attributes: {@code revWindowScale}, {@code fwdWindowScale}.
=======
  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableRollingProductSpec another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
>>>>>>> main
   * @return hashCode value
   */
  @Override
  public int hashCode() {
<<<<<<< HEAD
    int h = 5381;
    h += (h << 5) + revWindowScale.hashCode();
    h += (h << 5) + fwdWindowScale.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code RollingProductSpec} with attribute values.
=======
    return -332688391;
  }

  /**
   * Prints the immutable value {@code RollingProductSpec}.
>>>>>>> main
   * @return A string representation of the value
   */
  @Override
  public String toString() {
<<<<<<< HEAD
    return "RollingProductSpec{"
        + "revWindowScale=" + revWindowScale
        + ", fwdWindowScale=" + fwdWindowScale
        + "}";
  }

  private static ImmutableRollingProductSpec validate(ImmutableRollingProductSpec instance) {
    instance.checkWindowSizes();
    return instance;
=======
    return "RollingProductSpec{}";
>>>>>>> main
  }

  /**
   * Creates an immutable copy of a {@link RollingProductSpec} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable RollingProductSpec instance
   */
  public static ImmutableRollingProductSpec copyOf(RollingProductSpec instance) {
    if (instance instanceof ImmutableRollingProductSpec) {
      return (ImmutableRollingProductSpec) instance;
    }
    return ImmutableRollingProductSpec.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableRollingProductSpec ImmutableRollingProductSpec}.
   * <pre>
   * ImmutableRollingProductSpec.builder()
<<<<<<< HEAD
   *    .revWindowScale(io.deephaven.api.updateby.spec.WindowScale) // optional {@link RollingProductSpec#revWindowScale() revWindowScale}
   *    .fwdWindowScale(io.deephaven.api.updateby.spec.WindowScale) // optional {@link RollingProductSpec#fwdWindowScale() fwdWindowScale}
=======
>>>>>>> main
   *    .build();
   * </pre>
   * @return A new ImmutableRollingProductSpec builder
   */
  public static ImmutableRollingProductSpec.Builder builder() {
    return new ImmutableRollingProductSpec.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableRollingProductSpec ImmutableRollingProductSpec}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "RollingProductSpec", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
<<<<<<< HEAD
    private @Nullable WindowScale revWindowScale;
    private @Nullable WindowScale fwdWindowScale;
=======
>>>>>>> main

    private Builder() {
    }

    /**
<<<<<<< HEAD
     * Fill a builder with attribute values from the provided {@code io.deephaven.api.updateby.spec.RollingProductSpec} instance.
=======
     * Fill a builder with attribute values from the provided {@code RollingProductSpec} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
>>>>>>> main
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(RollingProductSpec instance) {
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
      if (object instanceof RollingProductSpec) {
        RollingProductSpec instance = (RollingProductSpec) object;
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
     * Initializes the value for the {@link RollingProductSpec#revWindowScale() revWindowScale} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link RollingProductSpec#revWindowScale() revWindowScale}.</em>
     * @param revWindowScale The value for revWindowScale 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder revWindowScale(WindowScale revWindowScale) {
      this.revWindowScale = Objects.requireNonNull(revWindowScale, "revWindowScale");
      return this;
    }

    /**
     * Initializes the value for the {@link RollingProductSpec#fwdWindowScale() fwdWindowScale} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link RollingProductSpec#fwdWindowScale() fwdWindowScale}.</em>
     * @param fwdWindowScale The value for fwdWindowScale 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder fwdWindowScale(WindowScale fwdWindowScale) {
      this.fwdWindowScale = Objects.requireNonNull(fwdWindowScale, "fwdWindowScale");
=======
>>>>>>> main
      return this;
    }

    /**
     * Builds a new {@link ImmutableRollingProductSpec ImmutableRollingProductSpec}.
     * @return An immutable instance of RollingProductSpec
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableRollingProductSpec build() {
<<<<<<< HEAD
      return ImmutableRollingProductSpec.validate(new ImmutableRollingProductSpec(this));
=======
      return new ImmutableRollingProductSpec(this);
>>>>>>> main
    }
  }
}
