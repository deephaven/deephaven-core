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
 */
public abstract class LiveAttributeMap<IFACE_TYPE extends AttributeMap<IFACE_TYPE>, IMPL_TYPE extends LiveAttributeMap<IFACE_TYPE, IMPL_TYPE>>
        extends LivenessArtifact
        implements AttributeMap<IFACE_TYPE> {

    private static final Map<String, Object> EMPTY_ATTRIBUTES = Collections.emptyMap();
    private static final AtomicReferenceFieldUpdater<LiveAttributeMap, Map> MUTABLE_ATTRIBUTES_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(LiveAttributeMap.class, Map.class, "mutableAttributes");
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
     */
    protected LiveAttributeMap(@Nullable final Map<String, Object> initialAttributes) {
        this.mutableAttributes = this.initialAttributes =
                Objects.requireNonNullElse(initialAttributes, EMPTY_ATTRIBUTES);
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
        if (needsManagement(object)) {
            manage((LivenessReferent) object);
        }
        ensureAttributes().put(key, object);
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
        final Map<String, Object> localAttributes = ensureAttributes();
        final Object currentValue = localAttributes.get(key);
        final Object updatedValue = Objects.requireNonNull(updater.apply(currentValue));
        if (currentValue == updatedValue) {
            return;
        }
        if (needsManagement(updatedValue)) {
            manage((LivenessReferent) updatedValue);
        }
        if (needsManagement(currentValue)) {
            unmanage((LivenessReferent) currentValue);
        }
        localAttributes.put(key, updatedValue);
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
        final Map<String, Object> localInitial = initialAttributes;
        if (localInitial == null) {
            // We've replaced the initial attributes already, no fanciness required.
            return mutableAttributes;
        }
        try {
            if (localInitial == EMPTY_ATTRIBUTES) {
                // noinspection unchecked
                return FieldUtils.ensureField(this, MUTABLE_ATTRIBUTES_UPDATER, EMPTY_ATTRIBUTES, HashMap::new);
            }
            // noinspection unchecked
            return FieldUtils.ensureField(this, MUTABLE_ATTRIBUTES_UPDATER, localInitial,
                    () -> new HashMap(localInitial));
        } finally {
            initialAttributes = null; // Avoid referencing initially-shared attributes for longer than necessary.
        }
    }

    /**
     * Access our own {@link #mutableAttributes} storage, requiring that {@link #ensureAttributes()} has been previously
     * invoked.
     *
     * @return The {@link #mutableAttributes} specific to {@code this}
     */
    private Map<String, Object> expectAttributes() {
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
        final Map<String, Object> localImmutableAttributes = immutableAttributes;
        if (localImmutableAttributes != null) {
            return localImmutableAttributes;
        }
        try {
            // noinspection unchecked
            return FieldUtils.ensureField(this, IMMUTABLE_ATTRIBUTES_UPDATER, null,
                    () -> Collections.unmodifiableMap(mutableAttributes));
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

    protected IFACE_TYPE prepareReturnCopy() {
        expectAttributes().values().forEach(av -> {
            if (needsManagement(av)) {
                manage((LivenessReferent) av);
            }
        });
        // noinspection unchecked
        return (IFACE_TYPE) this;
    }

    @Override
    public IFACE_TYPE withAttributes(
            @NotNull final Map<String, Object> toAdd,
            @NotNull final Collection<String> toRemove) {
        final Set<String> effectiveRemoves = new HashSet<>(toRemove);
        effectiveRemoves.removeAll(toAdd.keySet());

        final boolean addsSuperfluous = addsSuperfluous(toAdd);
        final boolean removesSuperfluous = removesSuperfluous(effectiveRemoves);
        if (addsSuperfluous && removesSuperfluous) {
            return prepareReturnThis();
        }

        final LiveAttributeMap<IFACE_TYPE, IMPL_TYPE> result = copy();
        if (!removesSuperfluous) {
            result.ensureAttributes().keySet().removeAll(effectiveRemoves);
        }
        if (!addsSuperfluous) {
            result.expectAttributes().putAll(toAdd);
        }

        return result.prepareReturnCopy();
    }

    @Override
    public IFACE_TYPE withAttributes(@NotNull final Map<String, Object> toAdd) {
        if (addsSuperfluous(toAdd)) {
            return prepareReturnThis();
        }

        final LiveAttributeMap<IFACE_TYPE, IMPL_TYPE> result = copy();
        result.ensureAttributes().putAll(toAdd);

        return result.prepareReturnCopy();
    }

    @Override
    public IFACE_TYPE withoutAttributes(@NotNull final Collection<String> toRemove) {
        if (removesSuperfluous(toRemove)) {
            return prepareReturnThis();
        }

        final LiveAttributeMap<IFACE_TYPE, IMPL_TYPE> result = copy();
        result.ensureAttributes().keySet().removeAll(toRemove);

        return result.prepareReturnCopy();
    }

    @Override
    public IFACE_TYPE retainingAttributes(@NotNull final Collection<String> toRetain) {
        if (retainsSuperfluous(toRetain)) {
            return prepareReturnThis();
        }

        final LiveAttributeMap<IFACE_TYPE, IMPL_TYPE> result = copy();
        result.ensureAttributes().keySet().retainAll(toRetain);

        return result.prepareReturnCopy();
    }

    /**
     * Create a copy of {@code this} with initially-shared {@link #mutableAttributes}.
     */
    protected abstract IMPL_TYPE copy();

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

    private static boolean needsManagement(@NotNull final Object object) {
        return object instanceof LivenessReferent && DynamicNode.notDynamicOrIsRefreshing(object);
    }
}
