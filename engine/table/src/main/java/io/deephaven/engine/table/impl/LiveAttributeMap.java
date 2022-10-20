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
import java.util.stream.Collectors;

/**
 * Re-usable {@link AttributeMap} implementation that is also a {@link LivenessArtifact}.
 * 
 * @implNote Rather than rely on {@code final}, explicitly-immutable {@link Map} instances for storage, this
 *           implementation does allow for mutation after construction. This allows a pattern wherein operations fill
 *           their result {@code AttributeMap} after construction using {@link #setAttribute(String, Object)}, which by
 *           convention must only be done before the result is published. No mutation is permitted after first access
 *           using any of {@link #getAttribute(String)}, {@link #getAttributeKeys()}, {@link #hasAttribute(String)},
 *           {@link #getAttributes()}, or {@link #getAttributes(Collection)}.
 */
public abstract class LiveAttributeMap<IFACE_TYPE extends AttributeMap<IFACE_TYPE>, IMPL_TYPE extends LiveAttributeMap<IFACE_TYPE, IMPL_TYPE>>
        extends LivenessArtifact
        implements AttributeMap<IFACE_TYPE> {

    private static final Map<String, Object> EMPTY_ATTRIBUTES = Collections.emptyMap();
    private static final AtomicReferenceFieldUpdater<LiveAttributeMap, Map> ATTRIBUTES_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(LiveAttributeMap.class, Map.class, "attributes");

    /**
     * Reference to a (possibly shared) initial map instance assigned to {@link #attributes}.
     */
    private Map<String, Object> initialAttributes;

    /**
     * Attribute storage, set via {@link #ensureAttributes()} on mutation if not initialized.
     */
    private volatile Map<String, Object> attributes;

    /**
     * @param initialAttributes The attributes map to use until mutated, or else {@code null} to allocate a new one
     */
    protected LiveAttributeMap(@Nullable final Map<String, Object> initialAttributes) {
        this.attributes = this.initialAttributes = Objects.requireNonNullElse(initialAttributes, EMPTY_ATTRIBUTES);
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
        if (object instanceof LivenessReferent && DynamicNode.notDynamicOrIsRefreshing(object)) {
            manage((LivenessReferent) object);
        }
        ensureAttributes().put(key, object);
    }

    /**
     * Ensure that we have our own {@link #attributes} storage.
     *
     * @return The {@link #attributes} specific to {@code this}
     */
    private Map<String, Object> ensureAttributes() {
        // If we see an "old" value, in the worst case we'll just try (and fail) to replace attributes.
        final Map<String, Object> localInitial = initialAttributes;
        if (localInitial == null) {
            // We've replaced the initial attributes already, no fanciness required.
            return attributes;
        }
        try {
            if (localInitial == EMPTY_ATTRIBUTES) {
                // noinspection unchecked
                return FieldUtils.ensureField(this, ATTRIBUTES_UPDATER, EMPTY_ATTRIBUTES, HashMap::new);
            }
            // noinspection unchecked
            return FieldUtils.ensureField(this, ATTRIBUTES_UPDATER, localInitial,
                    () -> new HashMap(localInitial));
        } finally {
            initialAttributes = null; // Avoid referencing initially-shared attributes for longer than necessary.
        }
    }

    /**
     * Access our own {@link #attributes} storage, requiring that {@link #ensureAttributes()} has been previously
     * invoked.
     *
     * @return The {@link #attributes} specific to {@code this}
     */
    private Map<String, Object> expectAttributes() {
        return Objects.requireNonNull(attributes);
    }

    /**
     * Ensure that our {@link #attributes} are immutable and will remain so.
     *
     * @return The {@link #attributes} specific to {@code this}, guaranteed to be immutable
     */
    private Map<String, Object> immutableAttributes() {
        // NB: Collections.unmodifiableMap returns its argument if that argument is already unmodifiable.
        Map<String, Object> localAttributes, immutableAttributes;
        while ((localAttributes = attributes) != (immutableAttributes = Collections.unmodifiableMap(localAttributes))) {
            if (ATTRIBUTES_UPDATER.compareAndSet(this, localAttributes, immutableAttributes)) {
                initialAttributes = null;
            }
        }
        return immutableAttributes;
    }

    private boolean addsSuperfluous(@NotNull final Map<String, Object> toAdd) {
        return toAdd.entrySet().stream().allMatch(ae -> {
            final String key = ae.getKey();
            final Object value = ae.getValue();
            return attributes.containsKey(key) && Objects.equals(attributes.get(key), value);
        });
    }

    private boolean removesSuperfluous(@NotNull final Collection<String> toRemove) {
        return toRemove.stream().noneMatch(ak -> attributes.containsKey(ak));
    }

    private boolean retainsSuperfluous(@NotNull final Collection<String> toRetain) {
        return toRetain.containsAll(attributes.keySet());
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
            if (av instanceof LivenessReferent && DynamicNode.notDynamicOrIsRefreshing(av)) {
                manage((LivenessReferent) av);
            }
        });
        // noinspection unchecked
        return (IFACE_TYPE) this;
    }

    @Override
    public IFACE_TYPE withAttributes(@NotNull final Map<String, Object> toAdd,
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
     * Create a copy of {@code this} with initially-shared {@link #attributes}.
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
        return getAttributes(Collections.emptySet());
    }

    @Override
    @ConcurrentMethod
    @NotNull
    public Map<String, Object> getAttributes(@Nullable final Collection<String> excluded) {
        if (excluded == null || excluded.isEmpty()) {
            return immutableAttributes();
        }
        return immutableAttributes().entrySet().stream()
                .filter(ae -> !excluded.contains(ae.getKey()))
                .collect(Collectors.collectingAndThen(
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue),
                        Collections::unmodifiableMap));
    }
}
