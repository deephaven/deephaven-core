package io.deephaven.engine.table;

import io.deephaven.api.util.ConcurrentMethod;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.liveness.LivenessScopeStack;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Interface for objects (mostly {@link Table tables} and similar) that act as an immutable map of key-value attribute
 * pairs.
 */
public interface AttributeMap<TYPE extends AttributeMap<TYPE>> {

    /**
     * Get an AttributeMap that is the same as {@code this}, but with the specified attributes added/replaced or
     * removed. If the supplied attributes {@code toAdd} and {@code toRemove} would not result in any changes to
     * {@code this}, implementations may return {@code this}.
     *
     * @param toAdd Attribute key-value pairs to add or replace (if the key already exists on {@code this}). Neither
     *        keys nor values may be {@code null}.
     * @param toRemove Attribute keys to remove
     * @return The result AttributeMap
     * @apiNote If {@code this} is a {@link Table}, the result will be a child {@link Table} that is identical but for
     *          its attributes, and if {@code ((Table)this).isRefreshing()}, the result will deliver identical
     *          {@link TableUpdate updates} to {@code this} on each cycle.
     * @apiNote If the result is a {@link LivenessReferent}, it will always be appropriately
     *          {@link io.deephaven.engine.liveness.LivenessManager#manage(LivenessReferent) managed} by the enclosing
     *          {@link LivenessScopeStack#peek() liveness scope}.
     */
    @ConcurrentMethod
    TYPE withAttributes(@NotNull Map<String, Object> toAdd, @NotNull Collection<String> toRemove);

    /**
     * Get an AttributeMap that is the same as {@code this}, but with the specified attributes added/replaced. If the
     * supplied attributes {@code toAdd} would not result in any changes to {@code this}, implementations may return
     * {@code this}.
     *
     * @param toAdd Attribute key-value pairs to add or replace (if the key already exists on {@code this})
     * @return The result AttributeMap
     * @apiNote If {@code this} is a {@link Table}, the result will be a child {@link Table} that is identical but for
     *          its attributes, and if {@code ((Table)this).isRefreshing()}, the result will deliver identical
     *          {@link TableUpdate updates} to {@code this} on each cycle.
     * @apiNote If the result is a {@link LivenessReferent}, it will always be appropriately
     *          {@link io.deephaven.engine.liveness.LivenessManager#manage(LivenessReferent) managed} by the enclosing
     *          {@link LivenessScopeStack#peek() liveness scope}.
     */
    @ConcurrentMethod
    TYPE withAttributes(@NotNull Map<String, Object> toAdd);

    /**
     * Get an AttributeMap that is the same as {@code this}, but with the specified attributes removed. If the supplied
     * attributes {@code toRemove} would not result in any changes to {@code this}, implementations may return
     * {@code this}.
     *
     * @param toRemove Attribute keys to remove
     * @return The result AttributeMap
     * @apiNote If {@code this} is a {@link Table}, the result will be a child {@link Table} that is identical but for
     *          its attributes, and if {@code ((Table)this).isRefreshing()}, the result will deliver identical
     *          {@link TableUpdate updates} to {@code this} on each cycle.
     * @apiNote If the result is a {@link LivenessReferent}, it will always be appropriately
     *          {@link io.deephaven.engine.liveness.LivenessManager#manage(LivenessReferent) managed} by the enclosing
     *          {@link LivenessScopeStack#peek() liveness scope}.
     */
    @ConcurrentMethod
    TYPE withoutAttributes(@NotNull Collection<String> toRemove);

    /**
     * Get an AttributeMap that is the same as {@code this}, but with only the specified attributes retained. If the
     * supplied attributes {@code toAdd} would not result in any changes to {@code this}, implementations may return
     * {@code this}.
     *
     * @param toRetain Attribute keys to retain
     * @return The result AttributeMap
     * @apiNote If {@code this} is a {@link Table}, the result will be a child {@link Table} that is identical but for
     *          its attributes, and if {@code ((Table)this).isRefreshing()}, the result will deliver identical
     *          {@link TableUpdate updates} to {@code this} on each cycle.
     * @apiNote If the result is a {@link LivenessReferent}, it will always be appropriately
     *          {@link io.deephaven.engine.liveness.LivenessManager#manage(LivenessReferent) managed} by the enclosing
     *          {@link LivenessScopeStack#peek() liveness scope}.
     */
    @ConcurrentMethod
    TYPE retainingAttributes(@NotNull Collection<String> toRetain);

    /**
     * Get the value for the specified attribute key.
     *
     * @param key The name of the attribute
     * @return The value, or {@code null} if there was none.
     */
    @ConcurrentMethod
    @Nullable
    Object getAttribute(@NotNull String key);

    /**
     * Get an immutable set of all the attributes that have values in this AttributeMap.
     *
     * @return An immutable set of attribute keys (names)
     */
    @ConcurrentMethod
    @NotNull
    Set<String> getAttributeKeys();

    /**
     * Check if the specified attribute exists in this AttributeMap.
     *
     * @param key The key (name) of the attribute
     * @return {@code true} if the attribute exists
     */
    @ConcurrentMethod
    boolean hasAttribute(@NotNull String key);

    /**
     * Get all attributes in this AttributeMap.
     *
     * @return An immutable map containing all attributes from this AttributeMap
     */
    @ConcurrentMethod
    @NotNull
    Map<String, Object> getAttributes();

    /**
     * Get all attributes from this AttributeMap except those whose keys appear in {@code excluded}.
     *
     * @param excluded A set of attributes keys (names) to exclude from the result
     * @return An immutable map containing AttributeMap's attributes, except those whose keys appear in {@code excluded}
     */
    @ConcurrentMethod
    @NotNull
    Map<String, Object> getAttributes(@Nullable Collection<String> excluded);
}
