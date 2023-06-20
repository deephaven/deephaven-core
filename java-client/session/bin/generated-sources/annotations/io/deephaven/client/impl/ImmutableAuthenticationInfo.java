package io.deephaven.client.impl;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
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
 * Immutable implementation of {@link AuthenticationInfo}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAuthenticationInfo.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableAuthenticationInfo.of()}.
 */
@Generated(from = "AuthenticationInfo", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableAuthenticationInfo extends AuthenticationInfo {
  private final String sessionHeaderKey;
  private final String session;

  private ImmutableAuthenticationInfo(String sessionHeaderKey, String session) {
    this.sessionHeaderKey = Objects.requireNonNull(sessionHeaderKey, "sessionHeaderKey");
    this.session = Objects.requireNonNull(session, "session");
  }

  private ImmutableAuthenticationInfo(ImmutableAuthenticationInfo original, String sessionHeaderKey, String session) {
    this.sessionHeaderKey = sessionHeaderKey;
    this.session = session;
  }

  /**
   * @return The value of the {@code sessionHeaderKey} attribute
   */
  @Override
  public String sessionHeaderKey() {
    return sessionHeaderKey;
  }

  /**
   * @return The value of the {@code session} attribute
   */
  @Override
  public String session() {
    return session;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AuthenticationInfo#sessionHeaderKey() sessionHeaderKey} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for sessionHeaderKey
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableAuthenticationInfo withSessionHeaderKey(String value) {
    String newValue = Objects.requireNonNull(value, "sessionHeaderKey");
    if (this.sessionHeaderKey.equals(newValue)) return this;
    return new ImmutableAuthenticationInfo(this, newValue, this.session);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AuthenticationInfo#session() session} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for session
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableAuthenticationInfo withSession(String value) {
    String newValue = Objects.requireNonNull(value, "session");
    if (this.session.equals(newValue)) return this;
    return new ImmutableAuthenticationInfo(this, this.sessionHeaderKey, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAuthenticationInfo} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAuthenticationInfo
        && equalTo(0, (ImmutableAuthenticationInfo) another);
  }

  private boolean equalTo(int synthetic, ImmutableAuthenticationInfo another) {
    return sessionHeaderKey.equals(another.sessionHeaderKey)
        && session.equals(another.session);
  }

  /**
   * Computes a hash code from attributes: {@code sessionHeaderKey}, {@code session}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + sessionHeaderKey.hashCode();
    h += (h << 5) + session.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code AuthenticationInfo} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("AuthenticationInfo")
        .omitNullValues()
        .add("sessionHeaderKey", sessionHeaderKey)
        .add("session", session)
        .toString();
  }

  /**
   * Construct a new immutable {@code AuthenticationInfo} instance.
   * @param sessionHeaderKey The value for the {@code sessionHeaderKey} attribute
   * @param session The value for the {@code session} attribute
   * @return An immutable AuthenticationInfo instance
   */
  public static ImmutableAuthenticationInfo of(String sessionHeaderKey, String session) {
    return new ImmutableAuthenticationInfo(sessionHeaderKey, session);
  }

  /**
   * Creates an immutable copy of a {@link AuthenticationInfo} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable AuthenticationInfo instance
   */
  public static ImmutableAuthenticationInfo copyOf(AuthenticationInfo instance) {
    if (instance instanceof ImmutableAuthenticationInfo) {
      return (ImmutableAuthenticationInfo) instance;
    }
    return ImmutableAuthenticationInfo.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAuthenticationInfo ImmutableAuthenticationInfo}.
   * <pre>
   * ImmutableAuthenticationInfo.builder()
   *    .sessionHeaderKey(String) // required {@link AuthenticationInfo#sessionHeaderKey() sessionHeaderKey}
   *    .session(String) // required {@link AuthenticationInfo#session() session}
   *    .build();
   * </pre>
   * @return A new ImmutableAuthenticationInfo builder
   */
  public static ImmutableAuthenticationInfo.Builder builder() {
    return new ImmutableAuthenticationInfo.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAuthenticationInfo ImmutableAuthenticationInfo}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AuthenticationInfo", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_SESSION_HEADER_KEY = 0x1L;
    private static final long INIT_BIT_SESSION = 0x2L;
    private long initBits = 0x3L;

    private @Nullable String sessionHeaderKey;
    private @Nullable String session;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AuthenticationInfo} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(AuthenticationInfo instance) {
      Objects.requireNonNull(instance, "instance");
      sessionHeaderKey(instance.sessionHeaderKey());
      session(instance.session());
      return this;
    }

    /**
     * Initializes the value for the {@link AuthenticationInfo#sessionHeaderKey() sessionHeaderKey} attribute.
     * @param sessionHeaderKey The value for sessionHeaderKey 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder sessionHeaderKey(String sessionHeaderKey) {
      this.sessionHeaderKey = Objects.requireNonNull(sessionHeaderKey, "sessionHeaderKey");
      initBits &= ~INIT_BIT_SESSION_HEADER_KEY;
      return this;
    }

    /**
     * Initializes the value for the {@link AuthenticationInfo#session() session} attribute.
     * @param session The value for session 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder session(String session) {
      this.session = Objects.requireNonNull(session, "session");
      initBits &= ~INIT_BIT_SESSION;
      return this;
    }

    /**
     * Builds a new {@link ImmutableAuthenticationInfo ImmutableAuthenticationInfo}.
     * @return An immutable instance of AuthenticationInfo
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAuthenticationInfo build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableAuthenticationInfo(null, sessionHeaderKey, session);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_SESSION_HEADER_KEY) != 0) attributes.add("sessionHeaderKey");
      if ((initBits & INIT_BIT_SESSION) != 0) attributes.add("session");
      return "Cannot build AuthenticationInfo, some of required attributes are not set " + attributes;
    }
  }
}
