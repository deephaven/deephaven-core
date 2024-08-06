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
 * Immutable implementation of {@link RollingFormulaSpec}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableRollingFormulaSpec.builder()}.
 */
@Generated(from = "RollingFormulaSpec", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableRollingFormulaSpec extends RollingFormulaSpec {
  private final WindowScale revWindowScale;
  private final WindowScale fwdWindowScale;
  private final String formula;
  private final String paramToken;

  private ImmutableRollingFormulaSpec(ImmutableRollingFormulaSpec.Builder builder) {
    this.formula = builder.formula;
    this.paramToken = builder.paramToken;
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

  private ImmutableRollingFormulaSpec(
      WindowScale revWindowScale,
      WindowScale fwdWindowScale,
      String formula,
      String paramToken) {
    this.revWindowScale = revWindowScale;
    this.fwdWindowScale = fwdWindowScale;
    this.formula = formula;
    this.paramToken = paramToken;
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "RollingFormulaSpec", generator = "Immutables")
  private final class InitShim {
    private byte revWindowScaleBuildStage = STAGE_UNINITIALIZED;
    private WindowScale revWindowScale;

    WindowScale revWindowScale() {
      if (revWindowScaleBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (revWindowScaleBuildStage == STAGE_UNINITIALIZED) {
        revWindowScaleBuildStage = STAGE_INITIALIZING;
        this.revWindowScale = Objects.requireNonNull(ImmutableRollingFormulaSpec.super.revWindowScale(), "revWindowScale");
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
        this.fwdWindowScale = Objects.requireNonNull(ImmutableRollingFormulaSpec.super.fwdWindowScale(), "fwdWindowScale");
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
      return "Cannot build RollingFormulaSpec, attribute initializers form cycle " + attributes;
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
   * @return The value of the {@code formula} attribute
   */
  @Override
  public String formula() {
    return formula;
  }

  /**
   * @return The value of the {@code paramToken} attribute
   */
  @Override
  public String paramToken() {
    return paramToken;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link RollingFormulaSpec#revWindowScale() revWindowScale} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for revWindowScale
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRollingFormulaSpec withRevWindowScale(WindowScale value) {
    if (this.revWindowScale == value) return this;
    WindowScale newValue = Objects.requireNonNull(value, "revWindowScale");
    return validate(new ImmutableRollingFormulaSpec(newValue, this.fwdWindowScale, this.formula, this.paramToken));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link RollingFormulaSpec#fwdWindowScale() fwdWindowScale} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for fwdWindowScale
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRollingFormulaSpec withFwdWindowScale(WindowScale value) {
    if (this.fwdWindowScale == value) return this;
    WindowScale newValue = Objects.requireNonNull(value, "fwdWindowScale");
    return validate(new ImmutableRollingFormulaSpec(this.revWindowScale, newValue, this.formula, this.paramToken));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link RollingFormulaSpec#formula() formula} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for formula
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRollingFormulaSpec withFormula(String value) {
    String newValue = Objects.requireNonNull(value, "formula");
    if (this.formula.equals(newValue)) return this;
    return validate(new ImmutableRollingFormulaSpec(this.revWindowScale, this.fwdWindowScale, newValue, this.paramToken));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link RollingFormulaSpec#paramToken() paramToken} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for paramToken
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRollingFormulaSpec withParamToken(String value) {
    String newValue = Objects.requireNonNull(value, "paramToken");
    if (this.paramToken.equals(newValue)) return this;
    return validate(new ImmutableRollingFormulaSpec(this.revWindowScale, this.fwdWindowScale, this.formula, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableRollingFormulaSpec} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableRollingFormulaSpec
        && equalTo(0, (ImmutableRollingFormulaSpec) another);
  }

  private boolean equalTo(int synthetic, ImmutableRollingFormulaSpec another) {
    return revWindowScale.equals(another.revWindowScale)
        && fwdWindowScale.equals(another.fwdWindowScale)
        && formula.equals(another.formula)
        && paramToken.equals(another.paramToken);
  }

  /**
   * Computes a hash code from attributes: {@code revWindowScale}, {@code fwdWindowScale}, {@code formula}, {@code paramToken}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + revWindowScale.hashCode();
    h += (h << 5) + fwdWindowScale.hashCode();
    h += (h << 5) + formula.hashCode();
    h += (h << 5) + paramToken.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code RollingFormulaSpec} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "RollingFormulaSpec{"
        + "revWindowScale=" + revWindowScale
        + ", fwdWindowScale=" + fwdWindowScale
        + ", formula=" + formula
        + ", paramToken=" + paramToken
        + "}";
  }

  private static ImmutableRollingFormulaSpec validate(ImmutableRollingFormulaSpec instance) {
    instance.checkParamToken();
    instance.checkFormula();
    instance.checkWindowSizes();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link RollingFormulaSpec} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable RollingFormulaSpec instance
   */
  public static ImmutableRollingFormulaSpec copyOf(RollingFormulaSpec instance) {
    if (instance instanceof ImmutableRollingFormulaSpec) {
      return (ImmutableRollingFormulaSpec) instance;
    }
    return ImmutableRollingFormulaSpec.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableRollingFormulaSpec ImmutableRollingFormulaSpec}.
   * <pre>
   * ImmutableRollingFormulaSpec.builder()
   *    .revWindowScale(io.deephaven.api.updateby.spec.WindowScale) // optional {@link RollingFormulaSpec#revWindowScale() revWindowScale}
   *    .fwdWindowScale(io.deephaven.api.updateby.spec.WindowScale) // optional {@link RollingFormulaSpec#fwdWindowScale() fwdWindowScale}
   *    .formula(String) // required {@link RollingFormulaSpec#formula() formula}
   *    .paramToken(String) // required {@link RollingFormulaSpec#paramToken() paramToken}
   *    .build();
   * </pre>
   * @return A new ImmutableRollingFormulaSpec builder
   */
  public static ImmutableRollingFormulaSpec.Builder builder() {
    return new ImmutableRollingFormulaSpec.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableRollingFormulaSpec ImmutableRollingFormulaSpec}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "RollingFormulaSpec", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_FORMULA = 0x1L;
    private static final long INIT_BIT_PARAM_TOKEN = 0x2L;
    private long initBits = 0x3L;

    private @Nullable WindowScale revWindowScale;
    private @Nullable WindowScale fwdWindowScale;
    private @Nullable String formula;
    private @Nullable String paramToken;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.api.updateby.spec.RollingFormulaSpec} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(RollingFormulaSpec instance) {
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
      if (object instanceof RollingFormulaSpec) {
        RollingFormulaSpec instance = (RollingFormulaSpec) object;
        formula(instance.formula());
        paramToken(instance.paramToken());
        if ((bits & 0x1L) == 0) {
          fwdWindowScale(instance.fwdWindowScale());
          bits |= 0x1L;
        }
        if ((bits & 0x2L) == 0) {
          revWindowScale(instance.revWindowScale());
          bits |= 0x2L;
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
     * Initializes the value for the {@link RollingFormulaSpec#revWindowScale() revWindowScale} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link RollingFormulaSpec#revWindowScale() revWindowScale}.</em>
     * @param revWindowScale The value for revWindowScale 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder revWindowScale(WindowScale revWindowScale) {
      this.revWindowScale = Objects.requireNonNull(revWindowScale, "revWindowScale");
      return this;
    }

    /**
     * Initializes the value for the {@link RollingFormulaSpec#fwdWindowScale() fwdWindowScale} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link RollingFormulaSpec#fwdWindowScale() fwdWindowScale}.</em>
     * @param fwdWindowScale The value for fwdWindowScale 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder fwdWindowScale(WindowScale fwdWindowScale) {
      this.fwdWindowScale = Objects.requireNonNull(fwdWindowScale, "fwdWindowScale");
      return this;
    }

    /**
     * Initializes the value for the {@link RollingFormulaSpec#formula() formula} attribute.
     * @param formula The value for formula 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder formula(String formula) {
      this.formula = Objects.requireNonNull(formula, "formula");
      initBits &= ~INIT_BIT_FORMULA;
      return this;
    }

    /**
     * Initializes the value for the {@link RollingFormulaSpec#paramToken() paramToken} attribute.
     * @param paramToken The value for paramToken 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder paramToken(String paramToken) {
      this.paramToken = Objects.requireNonNull(paramToken, "paramToken");
      initBits &= ~INIT_BIT_PARAM_TOKEN;
      return this;
    }

    /**
     * Builds a new {@link ImmutableRollingFormulaSpec ImmutableRollingFormulaSpec}.
     * @return An immutable instance of RollingFormulaSpec
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableRollingFormulaSpec build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableRollingFormulaSpec.validate(new ImmutableRollingFormulaSpec(this));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_FORMULA) != 0) attributes.add("formula");
      if ((initBits & INIT_BIT_PARAM_TOKEN) != 0) attributes.add("paramToken");
      return "Cannot build RollingFormulaSpec, some of required attributes are not set " + attributes;
    }
  }
}
