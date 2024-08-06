package io.deephaven.engine.context;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link QueryCompilerRequest}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableQueryCompilerRequest.builder()}.
 */
@Generated(from = "QueryCompilerRequest", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableQueryCompilerRequest extends QueryCompilerRequest {
  private final String description;
  private final String className;
  private final String classBody;
  private final String packageNameRoot;
  private final @Nullable StringBuilder codeLog;
  private final ImmutableMap<String, Class<?>> parameterClasses;

  private ImmutableQueryCompilerRequest(
      String description,
      String className,
      String classBody,
      String packageNameRoot,
      @Nullable StringBuilder codeLog,
      ImmutableMap<String, Class<?>> parameterClasses) {
    this.description = description;
    this.className = className;
    this.classBody = classBody;
    this.packageNameRoot = packageNameRoot;
    this.codeLog = codeLog;
    this.parameterClasses = parameterClasses;
  }

  /**
   * @return the description to add to the query performance recorder nugget for this request
   */
  @Override
  public String description() {
    return description;
  }

  /**
   * @return the class name to use for the generated class
   */
  @Override
  public String className() {
    return className;
  }

  /**
   * @return the class body, before update with "$CLASS_NAME$" replacement and package name prefixing
   */
  @Override
  public String classBody() {
    return classBody;
  }

  /**
   * @return the package name prefix
   */
  @Override
  public String packageNameRoot() {
    return packageNameRoot;
  }

  /**
   *Optional "log" for final class code. 
   */
  @Override
  public Optional<StringBuilder> codeLog() {
    return Optional.ofNullable(codeLog);
  }

  /**
   * @return the generic parameters, empty if none required
   */
  @Override
  public ImmutableMap<String, Class<?>> parameterClasses() {
    return parameterClasses;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link QueryCompilerRequest#description() description} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for description
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableQueryCompilerRequest withDescription(String value) {
    String newValue = Objects.requireNonNull(value, "description");
    if (this.description.equals(newValue)) return this;
    return new ImmutableQueryCompilerRequest(
        newValue,
        this.className,
        this.classBody,
        this.packageNameRoot,
        this.codeLog,
        this.parameterClasses);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link QueryCompilerRequest#className() className} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for className
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableQueryCompilerRequest withClassName(String value) {
    String newValue = Objects.requireNonNull(value, "className");
    if (this.className.equals(newValue)) return this;
    return new ImmutableQueryCompilerRequest(
        this.description,
        newValue,
        this.classBody,
        this.packageNameRoot,
        this.codeLog,
        this.parameterClasses);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link QueryCompilerRequest#classBody() classBody} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for classBody
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableQueryCompilerRequest withClassBody(String value) {
    String newValue = Objects.requireNonNull(value, "classBody");
    if (this.classBody.equals(newValue)) return this;
    return new ImmutableQueryCompilerRequest(
        this.description,
        this.className,
        newValue,
        this.packageNameRoot,
        this.codeLog,
        this.parameterClasses);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link QueryCompilerRequest#packageNameRoot() packageNameRoot} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for packageNameRoot
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableQueryCompilerRequest withPackageNameRoot(String value) {
    String newValue = Objects.requireNonNull(value, "packageNameRoot");
    if (this.packageNameRoot.equals(newValue)) return this;
    return new ImmutableQueryCompilerRequest(this.description, this.className, this.classBody, newValue, this.codeLog, this.parameterClasses);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link QueryCompilerRequest#codeLog() codeLog} attribute.
   * @param value The value for codeLog
   * @return A modified copy of {@code this} object
   */
  public final ImmutableQueryCompilerRequest withCodeLog(StringBuilder value) {
    @Nullable StringBuilder newValue = Objects.requireNonNull(value, "codeLog");
    if (this.codeLog == newValue) return this;
    return new ImmutableQueryCompilerRequest(
        this.description,
        this.className,
        this.classBody,
        this.packageNameRoot,
        newValue,
        this.parameterClasses);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link QueryCompilerRequest#codeLog() codeLog} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for codeLog
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableQueryCompilerRequest withCodeLog(Optional<? extends StringBuilder> optional) {
    @Nullable StringBuilder value = optional.orElse(null);
    if (this.codeLog == value) return this;
    return new ImmutableQueryCompilerRequest(
        this.description,
        this.className,
        this.classBody,
        this.packageNameRoot,
        value,
        this.parameterClasses);
  }

  /**
   * Copy the current immutable object by replacing the {@link QueryCompilerRequest#parameterClasses() parameterClasses} map with the specified map.
   * Nulls are not permitted as keys or values.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param entries The entries to be added to the parameterClasses map
   * @return A modified copy of {@code this} object
   */
  public final ImmutableQueryCompilerRequest withParameterClasses(Map<String, ? extends Class<?>> entries) {
    if (this.parameterClasses == entries) return this;
    ImmutableMap<String, Class<?>> newValue = ImmutableMap.copyOf(entries);
    return new ImmutableQueryCompilerRequest(this.description, this.className, this.classBody, this.packageNameRoot, this.codeLog, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableQueryCompilerRequest} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableQueryCompilerRequest
        && equalTo(0, (ImmutableQueryCompilerRequest) another);
  }

  private boolean equalTo(int synthetic, ImmutableQueryCompilerRequest another) {
    return description.equals(another.description)
        && className.equals(another.className)
        && classBody.equals(another.classBody)
        && packageNameRoot.equals(another.packageNameRoot)
        && Objects.equals(codeLog, another.codeLog)
        && parameterClasses.equals(another.parameterClasses);
  }

  /**
   * Computes a hash code from attributes: {@code description}, {@code className}, {@code classBody}, {@code packageNameRoot}, {@code codeLog}, {@code parameterClasses}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + description.hashCode();
    h += (h << 5) + className.hashCode();
    h += (h << 5) + classBody.hashCode();
    h += (h << 5) + packageNameRoot.hashCode();
    h += (h << 5) + Objects.hashCode(codeLog);
    h += (h << 5) + parameterClasses.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code QueryCompilerRequest} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("QueryCompilerRequest")
        .omitNullValues()
        .add("description", description)
        .add("className", className)
        .add("classBody", classBody)
        .add("packageNameRoot", packageNameRoot)
        .add("codeLog", codeLog)
        .add("parameterClasses", parameterClasses)
        .toString();
  }

  /**
   * Creates an immutable copy of a {@link QueryCompilerRequest} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable QueryCompilerRequest instance
   */
  public static ImmutableQueryCompilerRequest copyOf(QueryCompilerRequest instance) {
    if (instance instanceof ImmutableQueryCompilerRequest) {
      return (ImmutableQueryCompilerRequest) instance;
    }
    return ImmutableQueryCompilerRequest.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableQueryCompilerRequest ImmutableQueryCompilerRequest}.
   * <pre>
   * ImmutableQueryCompilerRequest.builder()
   *    .description(String) // required {@link QueryCompilerRequest#description() description}
   *    .className(String) // required {@link QueryCompilerRequest#className() className}
   *    .classBody(String) // required {@link QueryCompilerRequest#classBody() classBody}
   *    .packageNameRoot(String) // required {@link QueryCompilerRequest#packageNameRoot() packageNameRoot}
   *    .codeLog(StringBuilder) // optional {@link QueryCompilerRequest#codeLog() codeLog}
   *    .putParameterClasses|putAllParameterClasses(String =&gt; Class&amp;lt;?&amp;gt;) // {@link QueryCompilerRequest#parameterClasses() parameterClasses} mappings
   *    .build();
   * </pre>
   * @return A new ImmutableQueryCompilerRequest builder
   */
  public static ImmutableQueryCompilerRequest.Builder builder() {
    return new ImmutableQueryCompilerRequest.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableQueryCompilerRequest ImmutableQueryCompilerRequest}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "QueryCompilerRequest", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements QueryCompilerRequest.Builder {
    private static final long INIT_BIT_DESCRIPTION = 0x1L;
    private static final long INIT_BIT_CLASS_NAME = 0x2L;
    private static final long INIT_BIT_CLASS_BODY = 0x4L;
    private static final long INIT_BIT_PACKAGE_NAME_ROOT = 0x8L;
    private long initBits = 0xfL;

    private @Nullable String description;
    private @Nullable String className;
    private @Nullable String classBody;
    private @Nullable String packageNameRoot;
    private @Nullable StringBuilder codeLog;
    private ImmutableMap.Builder<String, Class<?>> parameterClasses = ImmutableMap.builder();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code QueryCompilerRequest} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(QueryCompilerRequest instance) {
      Objects.requireNonNull(instance, "instance");
      description(instance.description());
      className(instance.className());
      classBody(instance.classBody());
      packageNameRoot(instance.packageNameRoot());
      Optional<StringBuilder> codeLogOptional = instance.codeLog();
      if (codeLogOptional.isPresent()) {
        codeLog(codeLogOptional);
      }
      putAllParameterClasses(instance.parameterClasses());
      return this;
    }

    /**
     * Initializes the value for the {@link QueryCompilerRequest#description() description} attribute.
     * @param description The value for description 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder description(String description) {
      this.description = Objects.requireNonNull(description, "description");
      initBits &= ~INIT_BIT_DESCRIPTION;
      return this;
    }

    /**
     * Initializes the value for the {@link QueryCompilerRequest#className() className} attribute.
     * @param className The value for className 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder className(String className) {
      this.className = Objects.requireNonNull(className, "className");
      initBits &= ~INIT_BIT_CLASS_NAME;
      return this;
    }

    /**
     * Initializes the value for the {@link QueryCompilerRequest#classBody() classBody} attribute.
     * @param classBody The value for classBody 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder classBody(String classBody) {
      this.classBody = Objects.requireNonNull(classBody, "classBody");
      initBits &= ~INIT_BIT_CLASS_BODY;
      return this;
    }

    /**
     * Initializes the value for the {@link QueryCompilerRequest#packageNameRoot() packageNameRoot} attribute.
     * @param packageNameRoot The value for packageNameRoot 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder packageNameRoot(String packageNameRoot) {
      this.packageNameRoot = Objects.requireNonNull(packageNameRoot, "packageNameRoot");
      initBits &= ~INIT_BIT_PACKAGE_NAME_ROOT;
      return this;
    }

    /**
     * Initializes the optional value {@link QueryCompilerRequest#codeLog() codeLog} to codeLog.
     * @param codeLog The value for codeLog
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder codeLog(StringBuilder codeLog) {
      this.codeLog = Objects.requireNonNull(codeLog, "codeLog");
      return this;
    }

    /**
     * Initializes the optional value {@link QueryCompilerRequest#codeLog() codeLog} to codeLog.
     * @param codeLog The value for codeLog
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder codeLog(Optional<? extends StringBuilder> codeLog) {
      this.codeLog = codeLog.orElse(null);
      return this;
    }

    /**
     * Put one entry to the {@link QueryCompilerRequest#parameterClasses() parameterClasses} map.
     * @param key The key in the parameterClasses map
     * @param value The associated value in the parameterClasses map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder putParameterClasses(String key, Class<?> value) {
      this.parameterClasses.put(key, value);
      return this;
    }

    /**
     * Put one entry to the {@link QueryCompilerRequest#parameterClasses() parameterClasses} map. Nulls are not permitted
     * @param entry The key and value entry
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder putParameterClasses(Map.Entry<String, ? extends Class<?>> entry) {
      this.parameterClasses.put(entry);
      return this;
    }

    /**
     * Sets or replaces all mappings from the specified map as entries for the {@link QueryCompilerRequest#parameterClasses() parameterClasses} map. Nulls are not permitted
     * @param entries The entries that will be added to the parameterClasses map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder parameterClasses(Map<String, ? extends Class<?>> entries) {
      this.parameterClasses = ImmutableMap.builder();
      return putAllParameterClasses(entries);
    }

    /**
     * Put all mappings from the specified map as entries to {@link QueryCompilerRequest#parameterClasses() parameterClasses} map. Nulls are not permitted
     * @param entries The entries that will be added to the parameterClasses map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder putAllParameterClasses(Map<String, ? extends Class<?>> entries) {
      this.parameterClasses.putAll(entries);
      return this;
    }

    /**
     * Builds a new {@link ImmutableQueryCompilerRequest ImmutableQueryCompilerRequest}.
     * @return An immutable instance of QueryCompilerRequest
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableQueryCompilerRequest build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableQueryCompilerRequest(description, className, classBody, packageNameRoot, codeLog, parameterClasses.build());
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_DESCRIPTION) != 0) attributes.add("description");
      if ((initBits & INIT_BIT_CLASS_NAME) != 0) attributes.add("className");
      if ((initBits & INIT_BIT_CLASS_BODY) != 0) attributes.add("classBody");
      if ((initBits & INIT_BIT_PACKAGE_NAME_ROOT) != 0) attributes.add("packageNameRoot");
      return "Cannot build QueryCompilerRequest, some of required attributes are not set " + attributes;
    }
  }
}
