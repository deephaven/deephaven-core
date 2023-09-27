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
 * Immutable implementation of {@link RollingWAvgSpec}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableRollingWAvgSpec.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableRollingWAvgSpec.of()}.
 */
@Generated(from = "RollingWAvgSpec", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableRollingWAvgSpec extends RollingWAvgSpec {
<<<<<<< HEAD
  private final WindowScale revWindowScale;
  private final WindowScale fwdWindowScale;
=======
>>>>>>> main
  private final String weightCol;

  private ImmutableRollingWAvgSpec(String weightCol) {
    this.weightCol = Objects.requireNonNull(weightCol, "weightCol");
<<<<<<< HEAD
    this.revWindowScale = initShim.revWindowScale();
    this.fwdWindowScale = initShim.fwdWindowScale();
    this.initShim = null;
  }

  private ImmutableRollingWAvgSpec(ImmutableRollingWAvgSpec.Builder builder) {
    this.weightCol = builder.weightCol;
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

  private ImmutableRollingWAvgSpec(
      WindowScale revWindowScale,
      WindowScale fwdWindowScale,
      String weightCol) {
    this.revWindowScale = revWindowScale;
    this.fwdWindowScale = fwdWindowScale;
    this.weightCol = weightCol;
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "RollingWAvgSpec", generator = "Immutables")
  private final class InitShim {
    private byte revWindowScaleBuildStage = STAGE_UNINITIALIZED;
    private WindowScale revWindowScale;

    WindowScale revWindowScale() {
      if (revWindowScaleBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (revWindowScaleBuildStage == STAGE_UNINITIALIZED) {
        revWindowScaleBuildStage = STAGE_INITIALIZING;
        this.revWindowScale = Objects.requireNonNull(ImmutableRollingWAvgSpec.super.revWindowScale(), "revWindowScale");
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
        this.fwdWindowScale = Objects.requireNonNull(ImmutableRollingWAvgSpec.super.fwdWindowScale(), "fwdWindowScale");
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
      return "Cannot build RollingWAvgSpec, attribute initializers form cycle " + attributes;
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
  }

  private ImmutableRollingWAvgSpec(ImmutableRollingWAvgSpec original, String weightCol) {
    this.weightCol = weightCol;
>>>>>>> main
  }

  /**
   * @return The value of the {@code weightCol} attribute
   */
  @Override
  public String weightCol() {
    return weightCol;
  }

  /**
<<<<<<< HEAD
   * Copy the current immutable object by setting a value for the {@link RollingWAvgSpec#revWindowScale() revWindowScale} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for revWindowScale
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRollingWAvgSpec withRevWindowScale(WindowScale value) {
    if (this.revWindowScale == value) return this;
    WindowScale newValue = Objects.requireNonNull(value, "revWindowScale");
    return validate(new ImmutableRollingWAvgSpec(newValue, this.fwdWindowScale, this.weightCol));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link RollingWAvgSpec#fwdWindowScale() fwdWindowScale} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for fwdWindowScale
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRollingWAvgSpec withFwdWindowScale(WindowScale value) {
    if (this.fwdWindowScale == value) return this;
    WindowScale newValue = Objects.requireNonNull(value, "fwdWindowScale");
    return validate(new ImmutableRollingWAvgSpec(this.revWindowScale, newValue, this.weightCol));
  }

  /**
=======
>>>>>>> main
   * Copy the current immutable object by setting a value for the {@link RollingWAvgSpec#weightCol() weightCol} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for weightCol
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRollingWAvgSpec withWeightCol(String value) {
    String newValue = Objects.requireNonNull(value, "weightCol");
    if (this.weightCol.equals(newValue)) return this;
<<<<<<< HEAD
    return validate(new ImmutableRollingWAvgSpec(this.revWindowScale, this.fwdWindowScale, newValue));
=======
    return new ImmutableRollingWAvgSpec(this, newValue);
>>>>>>> main
  }

  /**
   * This instance is equal to all instances of {@code ImmutableRollingWAvgSpec} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableRollingWAvgSpec
        && equalTo(0, (ImmutableRollingWAvgSpec) another);
  }

  private boolean equalTo(int synthetic, ImmutableRollingWAvgSpec another) {
<<<<<<< HEAD
    return revWindowScale.equals(another.revWindowScale)
        && fwdWindowScale.equals(another.fwdWindowScale)
        && weightCol.equals(another.weightCol);
  }

  /**
   * Computes a hash code from attributes: {@code revWindowScale}, {@code fwdWindowScale}, {@code weightCol}.
=======
    return weightCol.equals(another.weightCol);
  }

  /**
   * Computes a hash code from attributes: {@code weightCol}.
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
    h += (h << 5) + weightCol.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code RollingWAvgSpec} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "RollingWAvgSpec{"
<<<<<<< HEAD
        + "revWindowScale=" + revWindowScale
        + ", fwdWindowScale=" + fwdWindowScale
        + ", weightCol=" + weightCol
=======
        + "weightCol=" + weightCol
>>>>>>> main
        + "}";
  }

  /**
   * Construct a new immutable {@code RollingWAvgSpec} instance.
   * @param weightCol The value for the {@code weightCol} attribute
   * @return An immutable RollingWAvgSpec instance
   */
  public static ImmutableRollingWAvgSpec of(String weightCol) {
<<<<<<< HEAD
    return validate(new ImmutableRollingWAvgSpec(weightCol));
  }

  private static ImmutableRollingWAvgSpec validate(ImmutableRollingWAvgSpec instance) {
    instance.checkWindowSizes();
    return instance;
=======
    return new ImmutableRollingWAvgSpec(weightCol);
>>>>>>> main
  }

  /**
   * Creates an immutable copy of a {@link RollingWAvgSpec} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable RollingWAvgSpec instance
   */
  public static ImmutableRollingWAvgSpec copyOf(RollingWAvgSpec instance) {
    if (instance instanceof ImmutableRollingWAvgSpec) {
      return (ImmutableRollingWAvgSpec) instance;
    }
    return ImmutableRollingWAvgSpec.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableRollingWAvgSpec ImmutableRollingWAvgSpec}.
   * <pre>
   * ImmutableRollingWAvgSpec.builder()
<<<<<<< HEAD
   *    .revWindowScale(io.deephaven.api.updateby.spec.WindowScale) // optional {@link RollingWAvgSpec#revWindowScale() revWindowScale}
   *    .fwdWindowScale(io.deephaven.api.updateby.spec.WindowScale) // optional {@link RollingWAvgSpec#fwdWindowScale() fwdWindowScale}
=======
>>>>>>> main
   *    .weightCol(String) // required {@link RollingWAvgSpec#weightCol() weightCol}
   *    .build();
   * </pre>
   * @return A new ImmutableRollingWAvgSpec builder
   */
  public static ImmutableRollingWAvgSpec.Builder builder() {
    return new ImmutableRollingWAvgSpec.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableRollingWAvgSpec ImmutableRollingWAvgSpec}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "RollingWAvgSpec", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_WEIGHT_COL = 0x1L;
    private long initBits = 0x1L;

<<<<<<< HEAD
    private @Nullable WindowScale revWindowScale;
    private @Nullable WindowScale fwdWindowScale;
=======
>>>>>>> main
    private @Nullable String weightCol;

    private Builder() {
    }

    /**
<<<<<<< HEAD
     * Fill a builder with attribute values from the provided {@code io.deephaven.api.updateby.spec.RollingWAvgSpec} instance.
=======
     * Fill a builder with attribute values from the provided {@code RollingWAvgSpec} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
>>>>>>> main
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(RollingWAvgSpec instance) {
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
      if (object instanceof RollingWAvgSpec) {
        RollingWAvgSpec instance = (RollingWAvgSpec) object;
        if ((bits & 0x2L) == 0) {
          revWindowScale(instance.revWindowScale());
          bits |= 0x2L;
        }
        if ((bits & 0x1L) == 0) {
          fwdWindowScale(instance.fwdWindowScale());
          bits |= 0x1L;
        }
        weightCol(instance.weightCol());
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
     * Initializes the value for the {@link RollingWAvgSpec#revWindowScale() revWindowScale} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link RollingWAvgSpec#revWindowScale() revWindowScale}.</em>
     * @param revWindowScale The value for revWindowScale 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder revWindowScale(WindowScale revWindowScale) {
      this.revWindowScale = Objects.requireNonNull(revWindowScale, "revWindowScale");
      return this;
    }

    /**
     * Initializes the value for the {@link RollingWAvgSpec#fwdWindowScale() fwdWindowScale} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link RollingWAvgSpec#fwdWindowScale() fwdWindowScale}.</em>
     * @param fwdWindowScale The value for fwdWindowScale 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder fwdWindowScale(WindowScale fwdWindowScale) {
      this.fwdWindowScale = Objects.requireNonNull(fwdWindowScale, "fwdWindowScale");
=======
      weightCol(instance.weightCol());
>>>>>>> main
      return this;
    }

    /**
     * Initializes the value for the {@link RollingWAvgSpec#weightCol() weightCol} attribute.
     * @param weightCol The value for weightCol 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder weightCol(String weightCol) {
      this.weightCol = Objects.requireNonNull(weightCol, "weightCol");
      initBits &= ~INIT_BIT_WEIGHT_COL;
      return this;
    }

    /**
     * Builds a new {@link ImmutableRollingWAvgSpec ImmutableRollingWAvgSpec}.
     * @return An immutable instance of RollingWAvgSpec
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableRollingWAvgSpec build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
<<<<<<< HEAD
      return ImmutableRollingWAvgSpec.validate(new ImmutableRollingWAvgSpec(this));
=======
      return new ImmutableRollingWAvgSpec(null, weightCol);
>>>>>>> main
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_WEIGHT_COL) != 0) attributes.add("weightCol");
      return "Cannot build RollingWAvgSpec, some of required attributes are not set " + attributes;
    }
  }
}
