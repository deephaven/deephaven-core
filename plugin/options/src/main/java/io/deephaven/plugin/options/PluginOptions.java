//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plugin.options;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.plugin.type.ObjectType;
import org.immutables.value.Value;

import io.deephaven.engine.validation.ColumnExpressionValidator;

import java.util.function.Predicate;

/**
 * A set of options for a plugin derived from dagger injection.
 */
@Value.Immutable
@BuildableStyle
public abstract class PluginOptions {
    /**
     * @return the {@link ColumnExpressionValidator} to use for user-provided formulas.
     */
    public abstract ColumnExpressionValidator columnExpressionValidator();

    /**
     * Returns {@code true} if the user should be permitted access to the given object.
     *
     * <p>
     * Callers should still apply {@link #authorizationTransformer()} before exporting an object to a user.
     * </p>
     */
    public abstract Predicate<Object> isAccessPermitted();

    public interface AuthorizationTransformer {
        <T> T transform(T object);
    }

    /**
     * Returns a function to be applied to objects returned to a user.
     *
     * <p>
     * Plugins may provide objects to the user by invoking
     * {@link io.deephaven.plugin.type.ObjectType.MessageStream#onData(java.nio.ByteBuffer, java.lang.Object...)} with a
     * number of arguments. These arguments are exported via an export ticket.
     *
     * A plugin may specify a {@link ObjectType.AuthorizationExportBehavior}. If
     * {@link ObjectType.AuthorizationExportBehavior#TRANSFORM} is set then the server automatically transforms objects
     * before sending them to a client. If the behavior is {@link ObjectType.AuthorizationExportBehavior#MANUAL}, then
     * the plugin must apply the authorization transform first - before passing it to the onData callback. If the
     * AuthorizationTransformer returns null, then the user is not permitted to access the object. If the behavior is
     * {@link ObjectType.AuthorizationExportBehavior#UNSET}, the behavior is dependent on the server's default.
     * </p>
     *
     * @return a {@link AuthorizationTransformer} that should be used to transform objects before providing them to a
     *         user.
     */
    public abstract AuthorizationTransformer authorizationTransformer();

    public static ImmutablePluginOptions.Builder builder() {
        return ImmutablePluginOptions.builder();
    }
}
