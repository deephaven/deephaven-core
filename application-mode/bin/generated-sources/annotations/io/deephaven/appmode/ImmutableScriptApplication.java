package io.deephaven.appmode;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Booleans;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.nio.file.Path;
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
 * Immutable implementation of {@link ScriptApplication}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableScriptApplication.builder()}.
 */
@Generated(from = "ScriptApplication", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableScriptApplication extends ScriptApplication {
  private final String id;
  private final String name;
  private final ImmutableList<Path> files;
  private final boolean isEnabled;
  private final String scriptType;

  private ImmutableScriptApplication(ImmutableScriptApplication.Builder builder) {
    this.id = builder.id;
    this.name = builder.name;
    this.files = builder.files.build();
    this.scriptType = builder.scriptType;
    this.isEnabled = builder.isEnabledIsSet()
        ? builder.isEnabled
        : super.isEnabled();
  }

  private ImmutableScriptApplication(
      String id,
      String name,
      ImmutableList<Path> files,
      boolean isEnabled,
      String scriptType) {
    this.id = id;
    this.name = name;
    this.files = files;
    this.isEnabled = isEnabled;
    this.scriptType = scriptType;
  }

  /**
   * @return The value of the {@code id} attribute
   */
  @Override
  public String id() {
    return id;
  }

  /**
   * @return The value of the {@code name} attribute
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * @return The value of the {@code files} attribute
   */
  @Override
  public ImmutableList<Path> files() {
    return files;
  }

  /**
   * @return The value of the {@code isEnabled} attribute
   */
  @Override
  public boolean isEnabled() {
    return isEnabled;
  }

  /**
   * @return The value of the {@code scriptType} attribute
   */
  @Override
  public String scriptType() {
    return scriptType;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ScriptApplication#id() id} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for id
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableScriptApplication withId(String value) {
    String newValue = Objects.requireNonNull(value, "id");
    if (this.id.equals(newValue)) return this;
    return validate(new ImmutableScriptApplication(newValue, this.name, this.files, this.isEnabled, this.scriptType));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ScriptApplication#name() name} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for name
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableScriptApplication withName(String value) {
    String newValue = Objects.requireNonNull(value, "name");
    if (this.name.equals(newValue)) return this;
    return validate(new ImmutableScriptApplication(this.id, newValue, this.files, this.isEnabled, this.scriptType));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ScriptApplication#files() files}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableScriptApplication withFiles(Path... elements) {
    ImmutableList<Path> newValue = ImmutableList.copyOf(elements);
    return validate(new ImmutableScriptApplication(this.id, this.name, newValue, this.isEnabled, this.scriptType));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ScriptApplication#files() files}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of files elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableScriptApplication withFiles(Iterable<? extends Path> elements) {
    if (this.files == elements) return this;
    ImmutableList<Path> newValue = ImmutableList.copyOf(elements);
    return validate(new ImmutableScriptApplication(this.id, this.name, newValue, this.isEnabled, this.scriptType));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ScriptApplication#isEnabled() isEnabled} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for isEnabled
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableScriptApplication withIsEnabled(boolean value) {
    if (this.isEnabled == value) return this;
    return validate(new ImmutableScriptApplication(this.id, this.name, this.files, value, this.scriptType));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ScriptApplication#scriptType() scriptType} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for scriptType
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableScriptApplication withScriptType(String value) {
    String newValue = Objects.requireNonNull(value, "scriptType");
    if (this.scriptType.equals(newValue)) return this;
    return validate(new ImmutableScriptApplication(this.id, this.name, this.files, this.isEnabled, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableScriptApplication} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableScriptApplication
        && equalTo(0, (ImmutableScriptApplication) another);
  }

  private boolean equalTo(int synthetic, ImmutableScriptApplication another) {
    return id.equals(another.id)
        && name.equals(another.name)
        && files.equals(another.files)
        && isEnabled == another.isEnabled
        && scriptType.equals(another.scriptType);
  }

  /**
   * Computes a hash code from attributes: {@code id}, {@code name}, {@code files}, {@code isEnabled}, {@code scriptType}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + id.hashCode();
    h += (h << 5) + name.hashCode();
    h += (h << 5) + files.hashCode();
    h += (h << 5) + Booleans.hashCode(isEnabled);
    h += (h << 5) + scriptType.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code ScriptApplication} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("ScriptApplication")
        .omitNullValues()
        .add("id", id)
        .add("name", name)
        .add("files", files)
        .add("isEnabled", isEnabled)
        .add("scriptType", scriptType)
        .toString();
  }

  private static ImmutableScriptApplication validate(ImmutableScriptApplication instance) {
    instance.checkNotEmpty();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link ScriptApplication} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ScriptApplication instance
   */
  public static ImmutableScriptApplication copyOf(ScriptApplication instance) {
    if (instance instanceof ImmutableScriptApplication) {
      return (ImmutableScriptApplication) instance;
    }
    return ImmutableScriptApplication.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableScriptApplication ImmutableScriptApplication}.
   * <pre>
   * ImmutableScriptApplication.builder()
   *    .id(String) // required {@link ScriptApplication#id() id}
   *    .name(String) // required {@link ScriptApplication#name() name}
   *    .addFiles|addAllFiles(java.nio.file.Path) // {@link ScriptApplication#files() files} elements
   *    .isEnabled(boolean) // optional {@link ScriptApplication#isEnabled() isEnabled}
   *    .scriptType(String) // required {@link ScriptApplication#scriptType() scriptType}
   *    .build();
   * </pre>
   * @return A new ImmutableScriptApplication builder
   */
  public static ImmutableScriptApplication.Builder builder() {
    return new ImmutableScriptApplication.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableScriptApplication ImmutableScriptApplication}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ScriptApplication", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements ScriptApplication.Builder {
    private static final long INIT_BIT_ID = 0x1L;
    private static final long INIT_BIT_NAME = 0x2L;
    private static final long INIT_BIT_SCRIPT_TYPE = 0x4L;
    private static final long OPT_BIT_IS_ENABLED = 0x1L;
    private long initBits = 0x7L;
    private long optBits;

    private @Nullable String id;
    private @Nullable String name;
    private ImmutableList.Builder<Path> files = ImmutableList.builder();
    private boolean isEnabled;
    private @Nullable String scriptType;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.appmode.ScriptApplication} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(ScriptApplication instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.appmode.ApplicationConfig} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(ApplicationConfig instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    private void from(Object object) {
      if (object instanceof ScriptApplication) {
        ScriptApplication instance = (ScriptApplication) object;
        name(instance.name());
        addAllFiles(instance.files());
        id(instance.id());
        scriptType(instance.scriptType());
      }
      if (object instanceof ApplicationConfig) {
        ApplicationConfig instance = (ApplicationConfig) object;
        isEnabled(instance.isEnabled());
      }
    }

    /**
     * Initializes the value for the {@link ScriptApplication#id() id} attribute.
     * @param id The value for id 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder id(String id) {
      this.id = Objects.requireNonNull(id, "id");
      initBits &= ~INIT_BIT_ID;
      return this;
    }

    /**
     * Initializes the value for the {@link ScriptApplication#name() name} attribute.
     * @param name The value for name 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder name(String name) {
      this.name = Objects.requireNonNull(name, "name");
      initBits &= ~INIT_BIT_NAME;
      return this;
    }

    /**
     * Adds one element to {@link ScriptApplication#files() files} list.
     * @param element A files element
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addFiles(Path element) {
      this.files.add(element);
      return this;
    }

    /**
     * Adds elements to {@link ScriptApplication#files() files} list.
     * @param elements An array of files elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addFiles(Path... elements) {
      this.files.add(elements);
      return this;
    }


    /**
     * Sets or replaces all elements for {@link ScriptApplication#files() files} list.
     * @param elements An iterable of files elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder files(Iterable<? extends Path> elements) {
      this.files = ImmutableList.builder();
      return addAllFiles(elements);
    }

    /**
     * Adds elements to {@link ScriptApplication#files() files} list.
     * @param elements An iterable of files elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addAllFiles(Iterable<? extends Path> elements) {
      this.files.addAll(elements);
      return this;
    }

    /**
     * Initializes the value for the {@link ScriptApplication#isEnabled() isEnabled} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link ScriptApplication#isEnabled() isEnabled}.</em>
     * @param isEnabled The value for isEnabled 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder isEnabled(boolean isEnabled) {
      this.isEnabled = isEnabled;
      optBits |= OPT_BIT_IS_ENABLED;
      return this;
    }

    /**
     * Initializes the value for the {@link ScriptApplication#scriptType() scriptType} attribute.
     * @param scriptType The value for scriptType 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder scriptType(String scriptType) {
      this.scriptType = Objects.requireNonNull(scriptType, "scriptType");
      initBits &= ~INIT_BIT_SCRIPT_TYPE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableScriptApplication ImmutableScriptApplication}.
     * @return An immutable instance of ScriptApplication
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableScriptApplication build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableScriptApplication.validate(new ImmutableScriptApplication(this));
    }

    private boolean isEnabledIsSet() {
      return (optBits & OPT_BIT_IS_ENABLED) != 0;
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_ID) != 0) attributes.add("id");
      if ((initBits & INIT_BIT_NAME) != 0) attributes.add("name");
      if ((initBits & INIT_BIT_SCRIPT_TYPE) != 0) attributes.add("scriptType");
      return "Cannot build ScriptApplication, some of required attributes are not set " + attributes;
    }
  }
}
