package io.deephaven.kafka.protobuf;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link DescriptorSchemaRegistry}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableDescriptorSchemaRegistry.builder()}.
 */
@Generated(from = "DescriptorSchemaRegistry", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableDescriptorSchemaRegistry
    extends DescriptorSchemaRegistry {
  private final String subject;
  private final @Nullable Integer version;
  private final @Nullable String messageName;

  private ImmutableDescriptorSchemaRegistry(
      String subject,
      @Nullable Integer version,
      @Nullable String messageName) {
    this.subject = subject;
    this.version = version;
    this.messageName = messageName;
  }

  /**
   * The schema subject to fetch from the schema registry.
   * @return the schema subject
   */
  @Override
  public String subject() {
    return subject;
  }

  /**
   * The schema version to fetch from the schema registry. When not set, the latest schema will be fetched.
   * <p>
   * For purposes of reproducibility across restarts where schema changes may occur, it is advisable for callers to
   * set this. This will ensure the resulting {@link io.deephaven.engine.table.TableDefinition table definition} will
   * not change across restarts. This gives the caller an explicit opportunity to update any downstream consumers
   * before bumping schema versions.
   * @return the schema version, or none for latest
   */
  @Override
  public OptionalInt version() {
    return version != null
        ? OptionalInt.of(version)
        : OptionalInt.empty();
  }

  /**
   * The fully-qualified protobuf {@link com.google.protobuf.Message} name, for example "com.example.MyMessage". This
   * message's {@link Descriptor} will be used as the basis for the resulting table's
   * {@link io.deephaven.engine.table.TableDefinition definition}. When not set, the first message descriptor in the
   * protobuf schema will be used.
   * <p>
   * It is advisable for callers to explicitly set this.
   * @return the schema message name
   */
  @Override
  public Optional<String> messageName() {
    return Optional.ofNullable(messageName);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DescriptorSchemaRegistry#subject() subject} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for subject
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDescriptorSchemaRegistry withSubject(String value) {
    String newValue = Objects.requireNonNull(value, "subject");
    if (this.subject.equals(newValue)) return this;
    return new ImmutableDescriptorSchemaRegistry(newValue, this.version, this.messageName);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link DescriptorSchemaRegistry#version() version} attribute.
   * @param value The value for version
   * @return A modified copy of {@code this} object
   */
  public final ImmutableDescriptorSchemaRegistry withVersion(int value) {
    @Nullable Integer newValue = value;
    if (Objects.equals(this.version, newValue)) return this;
    return new ImmutableDescriptorSchemaRegistry(this.subject, newValue, this.messageName);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link DescriptorSchemaRegistry#version() version} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for version
   * @return A modified copy of {@code this} object
   */
  public final ImmutableDescriptorSchemaRegistry withVersion(OptionalInt optional) {
    @Nullable Integer value = optional.isPresent() ? optional.getAsInt() : null;
    if (Objects.equals(this.version, value)) return this;
    return new ImmutableDescriptorSchemaRegistry(this.subject, value, this.messageName);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link DescriptorSchemaRegistry#messageName() messageName} attribute.
   * @param value The value for messageName
   * @return A modified copy of {@code this} object
   */
  public final ImmutableDescriptorSchemaRegistry withMessageName(String value) {
    @Nullable String newValue = Objects.requireNonNull(value, "messageName");
    if (Objects.equals(this.messageName, newValue)) return this;
    return new ImmutableDescriptorSchemaRegistry(this.subject, this.version, newValue);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link DescriptorSchemaRegistry#messageName() messageName} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for messageName
   * @return A modified copy of {@code this} object
   */
  public final ImmutableDescriptorSchemaRegistry withMessageName(Optional<String> optional) {
    @Nullable String value = optional.orElse(null);
    if (Objects.equals(this.messageName, value)) return this;
    return new ImmutableDescriptorSchemaRegistry(this.subject, this.version, value);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableDescriptorSchemaRegistry} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableDescriptorSchemaRegistry
        && equalTo(0, (ImmutableDescriptorSchemaRegistry) another);
  }

  private boolean equalTo(int synthetic, ImmutableDescriptorSchemaRegistry another) {
    return subject.equals(another.subject)
        && Objects.equals(version, another.version)
        && Objects.equals(messageName, another.messageName);
  }

  /**
   * Computes a hash code from attributes: {@code subject}, {@code version}, {@code messageName}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + subject.hashCode();
    h += (h << 5) + Objects.hashCode(version);
    h += (h << 5) + Objects.hashCode(messageName);
    return h;
  }

  /**
   * Prints the immutable value {@code DescriptorSchemaRegistry} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("DescriptorSchemaRegistry")
        .omitNullValues()
        .add("subject", subject)
        .add("version", version)
        .add("messageName", messageName)
        .toString();
  }

  /**
   * Creates an immutable copy of a {@link DescriptorSchemaRegistry} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable DescriptorSchemaRegistry instance
   */
  public static ImmutableDescriptorSchemaRegistry copyOf(DescriptorSchemaRegistry instance) {
    if (instance instanceof ImmutableDescriptorSchemaRegistry) {
      return (ImmutableDescriptorSchemaRegistry) instance;
    }
    return ImmutableDescriptorSchemaRegistry.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableDescriptorSchemaRegistry ImmutableDescriptorSchemaRegistry}.
   * <pre>
   * ImmutableDescriptorSchemaRegistry.builder()
   *    .subject(String) // required {@link DescriptorSchemaRegistry#subject() subject}
   *    .version(int) // optional {@link DescriptorSchemaRegistry#version() version}
   *    .messageName(String) // optional {@link DescriptorSchemaRegistry#messageName() messageName}
   *    .build();
   * </pre>
   * @return A new ImmutableDescriptorSchemaRegistry builder
   */
  public static ImmutableDescriptorSchemaRegistry.Builder builder() {
    return new ImmutableDescriptorSchemaRegistry.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableDescriptorSchemaRegistry ImmutableDescriptorSchemaRegistry}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "DescriptorSchemaRegistry", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements DescriptorSchemaRegistry.Builder {
    private static final long INIT_BIT_SUBJECT = 0x1L;
    private long initBits = 0x1L;

    private @Nullable String subject;
    private @Nullable Integer version;
    private @Nullable String messageName;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code DescriptorSchemaRegistry} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(DescriptorSchemaRegistry instance) {
      Objects.requireNonNull(instance, "instance");
      subject(instance.subject());
      OptionalInt versionOptional = instance.version();
      if (versionOptional.isPresent()) {
        version(versionOptional);
      }
      Optional<String> messageNameOptional = instance.messageName();
      if (messageNameOptional.isPresent()) {
        messageName(messageNameOptional);
      }
      return this;
    }

    /**
     * Initializes the value for the {@link DescriptorSchemaRegistry#subject() subject} attribute.
     * @param subject The value for subject 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder subject(String subject) {
      this.subject = Objects.requireNonNull(subject, "subject");
      initBits &= ~INIT_BIT_SUBJECT;
      return this;
    }

    /**
     * Initializes the optional value {@link DescriptorSchemaRegistry#version() version} to version.
     * @param version The value for version
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder version(int version) {
      this.version = version;
      return this;
    }

    /**
     * Initializes the optional value {@link DescriptorSchemaRegistry#version() version} to version.
     * @param version The value for version
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder version(OptionalInt version) {
      this.version = version.isPresent() ? version.getAsInt() : null;
      return this;
    }

    /**
     * Initializes the optional value {@link DescriptorSchemaRegistry#messageName() messageName} to messageName.
     * @param messageName The value for messageName
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder messageName(String messageName) {
      this.messageName = Objects.requireNonNull(messageName, "messageName");
      return this;
    }

    /**
     * Initializes the optional value {@link DescriptorSchemaRegistry#messageName() messageName} to messageName.
     * @param messageName The value for messageName
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder messageName(Optional<String> messageName) {
      this.messageName = messageName.orElse(null);
      return this;
    }

    /**
     * Builds a new {@link ImmutableDescriptorSchemaRegistry ImmutableDescriptorSchemaRegistry}.
     * @return An immutable instance of DescriptorSchemaRegistry
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableDescriptorSchemaRegistry build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableDescriptorSchemaRegistry(subject, version, messageName);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_SUBJECT) != 0) attributes.add("subject");
      return "Cannot build DescriptorSchemaRegistry, some of required attributes are not set " + attributes;
    }
  }
}
