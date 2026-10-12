//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.util.ConcurrentMethod;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.table.AttributeMap;
import io.deephaven.engine.table.impl.util.FieldUtils;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.util.annotations.InternalUseOnly;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * Re-usable {@link AttributeMap} implementation that is also a {@link LivenessArtifact}.
 * 
 * @implNote Rather than rely on {@code final}, explicitly-immutable {@link Map} instances for storage, this
 *           implementation does allow for mutation after construction. This allows a pattern wherein operations fill
 *           their result {@code AttributeMap} after construction using {@link #setAttribute(String, Object)}, which by
 *           convention must only be done before the result is published. No mutation is permitted after first access
 *           using any of {@link #getAttribute(String)}, {@link #getAttributeKeys()}, {@link #hasAttribute(String)},
 *           {@link #getAttributes()}, or {@link AttributeMap#getAttributes(Predicate)}.
 *           <p>
 *           Each attribute value that is a {@link LivenessReferent} and is either static or refreshing is managed by
 *           this map exactly once for as long as it remains in the map, including values supplied as initial
 *           attributes.
 */
public abstract class LiveAttributeMap<IFACE_TYPE extends AttributeMap<IFACE_TYPE>, IMPL_TYPE extends LiveAttributeMap<IFACE_TYPE, IMPL_TYPE>>
        extends LivenessArtifact
        implements AttributeMap<IFACE_TYPE> {

    private static final Map<String, Object> EMPTY_ATTRIBUTES = Collections.emptyMap();
    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<LiveAttributeMap, Map> MUTABLE_ATTRIBUTES_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(LiveAttributeMap.class, Map.class, "mutableAttributes");
    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<LiveAttributeMap, Map> IMMUTABLE_ATTRIBUTES_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(LiveAttributeMap.class, Map.class, "immutableAttributes");

    /**
     * Reference to a (possibly shared) initial map instance assigned to {@link #mutableAttributes}.
     */
    private Map<String, Object> initialAttributes;

    /**
     * Attribute storage while mutable, set via {@link #ensureAttributes()} on mutation if not initialized.
     */
    private volatile Map<String, Object> mutableAttributes;

    /**
     * Attribute storage once immutable, set via {@link #immutableAttributes()} on first read access from the public API
     * methods.
     */
    @SuppressWarnings("unused")
    private volatile Map<String, Object> immutableAttributes;

    /**
     * @param initialAttributes The attributes map to use until mutated, or else {@code null} to allocate a new one
     * @param enforceStrongReachability Whether this LiveAttributeMap should maintain strong references to its referents
     */
    protected LiveAttributeMap(@Nullable final Map<String, Object> initialAttributes,
            boolean enforceStrongReachability) {
        super(enforceStrongReachability);
        this.mutableAttributes = this.initialAttributes =
                Objects.requireNonNullElse(initialAttributes, EMPTY_ATTRIBUTES);
        this.initialAttributes.values().forEach(this::manageIfNeeded);
    }

    /**
     * Set the value of an attribute. This is for internal use by operations that build result AttributeMaps, and should
     * never be used from multiple threads or after a result has been published.
     *
     * @param key The name of the attribute; must not be {@code null}
     * @param object The value to be assigned; must not be {@code null}
     */
    @InternalUseOnly
    public void setAttribute(@NotNull final String key, @NotNull final Object object) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(object);
        final Object currentValue = currentAttributes().get(key);
        if (currentValue == object) {
            return;
        }
        replaceAttribute(key, currentValue, object);
    }

    /**
     * Read and update the value of an attribute. This is for internal use by operations that build result
     * AttributeMaps, and should never be used from multiple threads or after a result has been published.
     *
     * @param key The name of the attribute; must not be {@code null}
     * @param updater Function on the (possibly-{@code null}) existing value to produce the non-{@code null} new value
     */
    @InternalUseOnly
    public void setAttribute(@NotNull final String key, @NotNull final UnaryOperator<Object> updater) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(updater);
        final Object currentValue = currentAttributes().get(key);
        final Object updatedValue = Objects.requireNonNull(updater.apply(currentValue));
        if (currentValue == updatedValue) {
            return;
        }
        replaceAttribute(key, currentValue, updatedValue);
    }

    /**
     * Assign {@code newValue} to {@code key}, managing it and unmanaging {@code currentValue}, which it replaces.
     *
     * @param key The name of the attribute
     * @param currentValue The value currently assigned to {@code key}, or {@code null} if there is none
     * @param newValue The value to assign, which must not be {@code currentValue}
     */
    private void replaceAttribute(
            @NotNull final String key,
            @Nullable final Object currentValue,
            @NotNull final Object newValue) {
        manageIfNeeded(newValue);
        ensureAttributes().put(key, newValue);
        unmanageIfNeeded(currentValue);
    }

    /**
     * Copy attributes between AttributeMaps, filtered by a predicate.
     *
     * @param source The AttributeMap to copy attributes from
     * @param destination The LiveAttributeMap to copy attributes to
     * @param shouldCopy Should we copy this attribute key?
     */
    protected static void copyAttributes(
            @NotNull final AttributeMap<?> source,
            @NotNull final LiveAttributeMap<?, ?> destination,
            @NotNull final Predicate<String> shouldCopy) {
        for (final Map.Entry<String, Object> attrEntry : source.getAttributes().entrySet()) {
            final String attrName = attrEntry.getKey();
            if (shouldCopy.test(attrName)) {
                destination.setAttribute(attrName, attrEntry.getValue());
            }
        }
    }

    /**
     * Ensure that we have our own {@link #mutableAttributes} storage.
     *
     * @return The {@link #mutableAttributes} specific to {@code this}
     */
    private Map<String, Object> ensureAttributes() {
        checkMutable();
        // If we see an "old" value, in the worst case we'll just try (and fail) to replace attributes.
        final Map<String, Object> localInitialAttributes = initialAttributes;
        if (localInitialAttributes == null) {
            // We've replaced the initial attributes already, no fanciness required.
            return mutableAttributes;
        }
        try {
            // noinspection unchecked
            return FieldUtils.ensureField(this, MUTABLE_ATTRIBUTES_UPDATER, localInitialAttributes,
                    () -> localInitialAttributes.isEmpty()
                            ? new HashMap<>()
                            : new HashMap<>(localInitialAttributes));
        } finally {
            initialAttributes = null; // Avoid referencing initially-shared attributes for longer than necessary.
        }
    }

    /**
     * Access our {@link #mutableAttributes} for reading, without ensuring that they are specific to {@code this}.
     *
     * @return The current {@link #mutableAttributes}, which may still be the (possibly shared) initial attributes
     */
    private Map<String, Object> currentAttributes() {
        checkMutable();
        return Objects.requireNonNull(mutableAttributes);
    }

    /**
     * Ensure that our {@link #mutableAttributes} are immutable and will remain so.
     *
     * @return The {@link #mutableAttributes} specific to {@code this}, guaranteed to be immutable
     */
    private Map<String, Object> immutableAttributes() {
        // In JDK 17 and later, Collections.unmodifiableMap returns its argument if that argument is already
        // unmodifiable, although this behavior is not guaranteed. That allows an implementation wherein we test
        // if the map is unmodifiable by trying to make it unmodifiable and checking reference inequality with the
        // result, allowing us to avoid a separate instance member for immutable attributes.
        // See the following:
        // @formatter:off
        // Map<String, Object> localAttributes, immutableAttributes;
        // while ((localAttributes = attributes) != (immutableAttributes = Collections.unmodifiableMap(localAttributes))) {
        //     if (ATTRIBUTES_UPDATER.compareAndSet(this, localAttributes, immutableAttributes)) {
        //         initialAttributes = null;
        //     }
        // }
        // return immutableAttributes;
        // @formatter:on
        final Map<String, Object> localMutableAttributes = mutableAttributes;
        if (localMutableAttributes == null) {
            // We lost a race, someone else has already initialized immutableAttributes and cleared mutableAttributes.
            return Objects.requireNonNull(immutableAttributes);
        }
        try {
            // noinspection unchecked
            return FieldUtils.ensureField(this, IMMUTABLE_ATTRIBUTES_UPDATER, null,
                    () -> localMutableAttributes.isEmpty()
                            ? EMPTY_ATTRIBUTES
                            : Collections.unmodifiableMap(localMutableAttributes));
        } finally {
            mutableAttributes = null;
            initialAttributes = null;
        }
    }

    /**
     * Test if this LiveAttributeMap has been published yet. This determines whether it's safe to call
     * {@link #setAttribute(String, Object)} or {@link #setAttribute(String, UnaryOperator)}.
     * 
     * @return Whether this LiveAttributeMap has been published
     */
    public boolean published() {
        return immutableAttributes != null;
    }

    private void checkMutable() {
        if (immutableAttributes != null) {
            throw new UnsupportedOperationException("Cannot mutate attributes after they have been published");
        }
    }

    private boolean addsSuperfluous(@NotNull final Map<String, Object> toAdd) {
        final Map<String, Object> localImmutableAttributes = immutableAttributes();
        return toAdd.entrySet().stream().allMatch(ae -> {
            final String key = ae.getKey();
            final Object value = ae.getValue();
            return localImmutableAttributes.containsKey(key)
                    && Objects.equals(localImmutableAttributes.get(key), value);
        });
    }

    private boolean removesSuperfluous(@NotNull final Collection<String> toRemove) {
        final Map<String, Object> localImmutableAttributes = immutableAttributes();
        return toRemove.stream().noneMatch(localImmutableAttributes::containsKey);
    }

    private boolean retainsSuperfluous(@NotNull final Collection<String> toRetain) {
        return toRetain.containsAll(immutableAttributes().keySet());
    }

    protected IFACE_TYPE prepareReturnThis() {
        if (DynamicNode.notDynamicOrIsRefreshing(this)) {
            manageWithCurrentScope();
        }
        // noinspection unchecked
        return (IFACE_TYPE) this;
    }

    @Override
    public IFACE_TYPE withAttributes(
            @NotNull final Map<String, Object> toAdd,
            @NotNull final Collection<String> toRemove) {
        final Set<String> effectiveRemoves = new HashSet<>(toRemove);
        effectiveRemoves.removeAll(toAdd.keySet());

        if (addsSuperfluous(toAdd) && removesSuperfluous(effectiveRemoves)) {
            return prepareReturnThis();
        }

        final LiveAttributeMap<IFACE_TYPE, IMPL_TYPE> result =
                copy(buildAttributes(ak -> !effectiveRemoves.contains(ak), toAdd));
        result.reapplyAttributes(toAdd);
        result.removeAttributes(effectiveRemoves::contains);
        // noinspection unchecked
        return (IFACE_TYPE) result;
    }

    @Override
    public IFACE_TYPE withAttributes(@NotNull final Map<String, Object> toAdd) {
        if (addsSuperfluous(toAdd)) {
            return prepareReturnThis();
        }

        final LiveAttributeMap<IFACE_TYPE, IMPL_TYPE> result = copy(buildAttributes(ak -> true, toAdd));
        result.reapplyAttributes(toAdd);
        // noinspection unchecked
        return (IFACE_TYPE) result;
    }

    @Override
    public IFACE_TYPE withoutAttributes(@NotNull final Collection<String> toRemove) {
        if (removesSuperfluous(toRemove)) {
            return prepareReturnThis();
        }

        final Set<String> toRemoveSet = new HashSet<>(toRemove);
        final LiveAttributeMap<IFACE_TYPE, IMPL_TYPE> result =
                copy(buildAttributes(ak -> !toRemoveSet.contains(ak), Map.of()));
        result.removeAttributes(toRemoveSet::contains);
        // noinspection unchecked
        return (IFACE_TYPE) result;
    }

    @Override
    public IFACE_TYPE retainingAttributes(@NotNull final Collection<String> toRetain) {
        if (retainsSuperfluous(toRetain)) {
            return prepareReturnThis();
        }

        final Set<String> toRetainSet = new HashSet<>(toRetain);
        final LiveAttributeMap<IFACE_TYPE, IMPL_TYPE> result = copy(buildAttributes(toRetainSet::contains, Map.of()));
        result.removeAttributes(ak -> !toRetainSet.contains(ak));
        // noinspection unchecked
        return (IFACE_TYPE) result;
    }

    /**
     * Get our attributes whose keys satisfy {@code shouldCopy}, for another map to use as its initial attributes.
     *
     * @param shouldCopy Should we copy the attribute with this key?
     * @return Our own published attributes if every key satisfies {@code shouldCopy}, else a new map of those that do,
     *         which is never mutated, since {@link #ensureAttributes()} replaces initial attributes before any change
     */
    protected final Map<String, Object> getAttributesToCopy(@NotNull final Predicate<String> shouldCopy) {
        final Map<String, Object> localImmutableAttributes = immutableAttributes();
        if (localImmutableAttributes.keySet().stream().allMatch(shouldCopy)) {
            return localImmutableAttributes;
        }
        return buildAttributes(shouldCopy, Map.of());
    }

    /**
     * Build the attributes for a copy of {@code this}: our attributes whose keys satisfy {@code shouldKeep}, overlaid
     * with {@code toAdd}.
     *
     * @param shouldKeep Should we keep our attribute with this key?
     * @param toAdd Attributes to add, replacing any of ours with the same keys
     * @return A new map of the resulting attributes, which only the copy's constructor receives and which is never
     *         mutated, since {@link #ensureAttributes()} replaces initial attributes before any change
     */
    private Map<String, Object> buildAttributes(
            @NotNull final Predicate<String> shouldKeep,
            @NotNull final Map<String, Object> toAdd) {
        final Map<String, Object> localImmutableAttributes = immutableAttributes();
        final Map<String, Object> result = new HashMap<>(localImmutableAttributes.size() + toAdd.size());
        localImmutableAttributes.forEach((ak, av) -> {
            if (shouldKeep.test(ak)) {
                result.put(ak, av);
            }
        });
        result.putAll(toAdd);
        return result.isEmpty() ? EMPTY_ATTRIBUTES : result;
    }

    /**
     * Assign each of {@code toAdd}'s values to its key. A copy's constructor may replace or drop attributes it was
     * given, as {@code BaseTable} does with {@link io.deephaven.engine.table.Table#SYSTEMIC_TABLE_ATTRIBUTE} according
     * to whether the current thread is systemic; this restores any such attribute that the caller asked to add. It does
     * nothing, and in particular does not call {@link #ensureAttributes()}, when every value is already assigned.
     *
     * @param toAdd The attributes that the caller asked to add
     */
    private void reapplyAttributes(@NotNull final Map<String, Object> toAdd) {
        toAdd.forEach(this::setAttribute);
    }

    /**
     * Remove the attributes whose keys satisfy {@code shouldRemove}, unmanaging their values. A copy's constructor may
     * add attributes beyond the ones it was given, as {@code BaseTable} does with
     * {@link io.deephaven.engine.table.Table#SYSTEMIC_TABLE_ATTRIBUTE} on a systemic thread; this removes any such
     * attribute that the caller asked to be absent. It does nothing, and in particular does not call
     * {@link #ensureAttributes()}, when no key satisfies {@code shouldRemove}.
     *
     * @param shouldRemove Should we remove the attribute with this key?
     */
    private void removeAttributes(@NotNull final Predicate<String> shouldRemove) {
        if (currentAttributes().keySet().stream().noneMatch(shouldRemove)) {
            return;
        }
        final List<LivenessReferent> removedReferents = new ArrayList<>();
        for (final Iterator<Map.Entry<String, Object>> it = ensureAttributes().entrySet().iterator(); it.hasNext();) {
            final Map.Entry<String, Object> attrEntry = it.next();
            if (!shouldRemove.test(attrEntry.getKey())) {
                continue;
            }
            final Object removedValue = attrEntry.getValue();
            it.remove();
            if (needsManagement(removedValue)) {
                removedReferents.add((LivenessReferent) removedValue);
            }
        }
        if (!removedReferents.isEmpty()) {
            unmanage(removedReferents.stream());
        }
    }

    /**
     * Create a copy of {@code this} that is constructed with {@code attributes} as its initial attributes.
     *
     * @param attributes The attributes for the copy, which implementations must pass to its constructor without
     *        modifying
     * @return The copy
     */
    protected abstract IMPL_TYPE copy(@NotNull Map<String, Object> attributes);

    @Override
    @ConcurrentMethod
    @Nullable
    public Object getAttribute(@NotNull final String key) {
        return immutableAttributes().get(key);
    }

    @Override
    @ConcurrentMethod
    @NotNull
    public Set<String> getAttributeKeys() {
        return immutableAttributes().keySet();
    }

    @Override
    @ConcurrentMethod
    public boolean hasAttribute(@NotNull final String name) {
        return immutableAttributes().containsKey(name);
    }

    @Override
    @NotNull
    public Map<String, Object> getAttributes() {
        return immutableAttributes();
    }

    @Override
    @ConcurrentMethod
    @NotNull
    public Map<String, Object> getAttributes(@NotNull final Predicate<String> included) {
        return immutableAttributes().entrySet().stream()
                .filter(ae -> included.test(ae.getKey()))
                .collect(Collectors.collectingAndThen(
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue),
                        Collections::unmodifiableMap));
    }

    private void manageIfNeeded(@Nullable final Object object) {
        if (needsManagement(object)) {
            manage((LivenessReferent) object);
        }
    }

    private void unmanageIfNeeded(@Nullable final Object object) {
        if (needsManagement(object)) {
            unmanage((LivenessReferent) object);
        }
    }

    private static boolean needsManagement(@Nullable final Object object) {
        return object instanceof LivenessReferent && DynamicNode.notDynamicOrIsRefreshing(object);
    }
}
