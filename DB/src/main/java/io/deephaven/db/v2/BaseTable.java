/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.base.Base64;
import io.deephaven.base.StringUtils;
import io.deephaven.hash.KeyedObjectHashSet;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.reference.WeakSimpleReference;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.io.logger.Logger;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.NotSortableException;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.live.NotificationQueue;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.tables.utils.QueryPerformanceRecorder;
import io.deephaven.db.tables.utils.SystemicObjectTracker;
import io.deephaven.db.util.ColumnFormattingValues;
import io.deephaven.db.util.liveness.LivenessArtifact;
import io.deephaven.db.util.liveness.LivenessReferent;
import io.deephaven.db.v2.remote.ConstructSnapshot;
import io.deephaven.db.v2.select.SelectColumn;
import io.deephaven.db.v2.select.SourceColumn;
import io.deephaven.db.v2.select.SwitchColumn;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.LogicalClock;
import io.deephaven.db.v2.utils.*;
import io.deephaven.util.annotations.ReferentialIntegrity;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.util.datastructures.SimpleReferenceManager;
import io.deephaven.util.datastructures.hash.IdentityKeyedObjectKey;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base abstract class all standard table implementations.
 */
public abstract class BaseTable extends LivenessArtifact
        implements DynamicTable, Serializable, NotificationStepReceiver, NotificationStepSource {

    private static final long serialVersionUID = 1L;

    private static final boolean VALIDATE_UPDATE_INDICES =
            Configuration.getInstance().getBooleanWithDefault("BaseTable.validateUpdateIndices", false);
    private static final boolean VALIDATE_UPDATE_OVERLAPS =
            Configuration.getInstance().getBooleanWithDefault("BaseTable.validateUpdateOverlaps", true);
    public static final boolean PRINT_SERIALIZED_UPDATE_OVERLAPS =
            Configuration.getInstance().getBooleanWithDefault("BaseTable.printSerializedUpdateOverlaps", false);

    private static final Logger log = LoggerFactory.getLogger(BaseTable.class);

    /**
     * This table's definition.
     */
    protected final TableDefinition definition;

    /**
     * This table's description.
     */
    protected final String description;

    // Attribute support
    protected final ConcurrentHashMap<String, Object> attributes = new ConcurrentHashMap<>();

    // Fields for DynamicTable implementation
    private transient boolean refreshing;
    private transient Condition liveTableMonitorCondition;
    private transient Collection<Object> parents;
    private transient SimpleReferenceManager<Listener, WeakSimpleReference<Listener>> childListenerReferences;
    private transient SimpleReferenceManager<Listener, WeakSimpleReference<Listener>> directChildListenerReferences;
    private transient SimpleReferenceManager<ShiftAwareListener, WeakSimpleReference<ShiftAwareListener>> childShiftAwareListenerReferences;
    private transient volatile long lastNotificationStep;
    private transient volatile long lastSatisfiedStep;
    private transient boolean isFailed;

    public BaseTable(@NotNull final TableDefinition definition, @NotNull final String description) {
        this.definition = definition;
        this.description = description;
        initializeTransientFields();
        initializeSystemicAttribute();
    }

    private void initializeSystemicAttribute() {
        if (SystemicObjectTracker.isSystemicThread()) {
            markSystemic();
        }
    }

    private void initializeTransientFields() {
        refreshing = false;
        isFailed = false;
        liveTableMonitorCondition = LiveTableMonitor.DEFAULT.exclusiveLock().newCondition();
        parents = new KeyedObjectHashSet<>(IdentityKeyedObjectKey.getInstance());
        childListenerReferences = new SimpleReferenceManager<>(WeakSimpleReference::new, true);
        directChildListenerReferences = new SimpleReferenceManager<>(WeakSimpleReference::new, true);
        childShiftAwareListenerReferences = new SimpleReferenceManager<>(WeakSimpleReference::new, true);
        lastNotificationStep = LogicalClock.DEFAULT.currentStep();
    }

    private void readObject(ObjectInputStream objectInputStream) throws IOException, ClassNotFoundException {
        objectInputStream.defaultReadObject();
        initializeTransientFields();
    }

    // ------------------------------------------------------------------------------------------------------------------
    // Metadata Operation Implementations
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    public TableDefinition getDefinition() {
        return definition;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return description;
    }

    @Override
    public LogOutput append(@NotNull final LogOutput logOutput) {
        return logOutput.append(description);
    }

    // ------------------------------------------------------------------------------------------------------------------
    // Attribute Operation Implementations
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    public void setAttribute(@NotNull final String key, final Object object) {
        if (object instanceof LivenessReferent && DynamicNode.notDynamicOrIsRefreshing(object)) {
            manage((LivenessReferent) object);
        }
        attributes.put(key, object);
    }

    @Override
    public Object getAttribute(@NotNull String key) {
        return attributes.get(key);
    }

    @Override
    @NotNull
    public Set<String> getAttributeNames() {
        return new HashSet<>(attributes.keySet());
    }

    @Override
    public boolean hasAttribute(@NotNull final String name) {
        return attributes.containsKey(name);
    }

    @Override
    public Map<String, Object> getAttributes(Collection<String> excludedAttrs) {
        // If nothing is excluded, don't make more garbage
        if (excludedAttrs == null || excludedAttrs.isEmpty()) {
            return Collections.unmodifiableMap(attributes);
        }

        return Collections
                .unmodifiableMap(attributes.entrySet().stream().filter(ent -> !excludedAttrs.contains(ent.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    public enum CopyAttributeOperation {
        // Do not copy any attributes
        None,

        // legacy attributes
        Flatten, Sort, UpdateView, Join, Filter,
        // new attributes
        DropColumns, View, Reverse,
        /**
         * The result tables that go in a ByExternal TableMap
         */
        ByExternal, Coalesce, WouldMatch, LastBy, FirstBy,

        // Hierarchical attributes
        Rollup, Treetable,

        // Copy attributes
        RollupCopy, TreetableCopy,

        Preview
    }

    private static final Map<String, EnumSet<CopyAttributeOperation>> attributeToCopySet;
    private static final EnumSet<CopyAttributeOperation> LEGACY_COPY_ATTRIBUTES = EnumSet.of(
            CopyAttributeOperation.Flatten,
            CopyAttributeOperation.Sort,
            CopyAttributeOperation.UpdateView,
            CopyAttributeOperation.Join,
            CopyAttributeOperation.Filter);
    static {
        final HashMap<String, EnumSet<CopyAttributeOperation>> tempMap = new HashMap<>();

        // the existing attributes would have been copied for these operations
        tempMap.put(INPUT_TABLE_ATTRIBUTE, LEGACY_COPY_ATTRIBUTES);
        tempMap.put(DO_NOT_MAKE_REMOTE_ATTRIBUTE, LEGACY_COPY_ATTRIBUTES);

        // byExternal was creating the sub table with a bespoke ACL copy; we should copy ACLs there in addition to the
        // legacy attributes
        final EnumSet<CopyAttributeOperation> aclCopyAttributes = EnumSet.copyOf(LEGACY_COPY_ATTRIBUTES);
        aclCopyAttributes.addAll(EnumSet.of(
                CopyAttributeOperation.FirstBy,
                CopyAttributeOperation.LastBy,
                CopyAttributeOperation.ByExternal,
                CopyAttributeOperation.WouldMatch));

        // for a merged table, we'll allow operations that keep our Index + column sources the same to break us down
        // into constituent tables
        tempMap.put(MERGED_TABLE_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.DropColumns,
                CopyAttributeOperation.View));

        tempMap.put(EMPTY_SOURCE_TABLE_ATTRIBUTE, EnumSet.complementOf(EnumSet.of(
                CopyAttributeOperation.Rollup,
                CopyAttributeOperation.Treetable)));
        tempMap.put(SORTABLE_COLUMNS_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.Flatten,
                CopyAttributeOperation.Sort,
                CopyAttributeOperation.Join,
                CopyAttributeOperation.Filter,
                CopyAttributeOperation.Reverse,
                CopyAttributeOperation.Coalesce,
                CopyAttributeOperation.ByExternal,
                CopyAttributeOperation.WouldMatch,
                CopyAttributeOperation.Preview));

        tempMap.put(FILTERABLE_COLUMNS_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.Flatten,
                CopyAttributeOperation.Sort,
                CopyAttributeOperation.Join,
                CopyAttributeOperation.Filter,
                CopyAttributeOperation.Reverse,
                CopyAttributeOperation.Coalesce,
                CopyAttributeOperation.ByExternal,
                CopyAttributeOperation.WouldMatch,
                CopyAttributeOperation.Preview));

        tempMap.put(LAYOUT_HINTS_ATTRIBUTE, EnumSet.allOf(CopyAttributeOperation.class));

        tempMap.put(TOTALS_TABLE_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.Flatten,
                CopyAttributeOperation.Sort,
                CopyAttributeOperation.Filter,
                CopyAttributeOperation.Reverse,
                CopyAttributeOperation.ByExternal,
                CopyAttributeOperation.Coalesce));

        tempMap.put(SYSTEMIC_TABLE_ATTRIBUTE, EnumSet.of(CopyAttributeOperation.None));

        // Column renderers propagate for all operations that will not rename, remove, or change column types
        tempMap.put(COLUMN_RENDERERS_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.Flatten,
                CopyAttributeOperation.Sort,
                CopyAttributeOperation.Filter,
                CopyAttributeOperation.Reverse,
                CopyAttributeOperation.ByExternal,
                CopyAttributeOperation.Coalesce,
                CopyAttributeOperation.LastBy,
                CopyAttributeOperation.FirstBy,
                CopyAttributeOperation.Treetable,
                CopyAttributeOperation.Preview));

        // Tree table attributes
        tempMap.put(HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.DropColumns,
                CopyAttributeOperation.Sort,
                CopyAttributeOperation.Filter,
                CopyAttributeOperation.Reverse,
                CopyAttributeOperation.Coalesce,
                CopyAttributeOperation.UpdateView,
                CopyAttributeOperation.ByExternal,
                CopyAttributeOperation.Flatten,
                CopyAttributeOperation.RollupCopy));

        tempMap.put(ROLLUP_LEAF_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.DropColumns,
                CopyAttributeOperation.Sort,
                CopyAttributeOperation.Filter,
                CopyAttributeOperation.Reverse,
                CopyAttributeOperation.Coalesce,
                CopyAttributeOperation.UpdateView,
                CopyAttributeOperation.ByExternal,
                CopyAttributeOperation.Flatten));

        tempMap.put(TREE_TABLE_FILTER_REVERSE_LOOKUP_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.DropColumns,
                CopyAttributeOperation.RollupCopy));

        tempMap.put(HIERARCHICAL_SOURCE_TABLE_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.Sort,
                CopyAttributeOperation.UpdateView,
                CopyAttributeOperation.DropColumns,
                CopyAttributeOperation.RollupCopy));

        tempMap.put(HIERARCHICAL_SOURCE_INFO_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.Sort,
                CopyAttributeOperation.DropColumns,
                CopyAttributeOperation.UpdateView,
                CopyAttributeOperation.RollupCopy));

        tempMap.put(REVERSE_LOOKUP_ATTRIBUTE, EnumSet.of(CopyAttributeOperation.RollupCopy));

        tempMap.put(PREPARED_RLL_ATTRIBUTE, EnumSet.of(CopyAttributeOperation.Filter));

        tempMap.put(COLUMN_DESCRIPTIONS_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.Flatten,
                CopyAttributeOperation.Sort,
                CopyAttributeOperation.Filter,
                CopyAttributeOperation.Reverse,
                CopyAttributeOperation.ByExternal,
                CopyAttributeOperation.Coalesce,
                CopyAttributeOperation.FirstBy,
                CopyAttributeOperation.LastBy,
                CopyAttributeOperation.Treetable,
                CopyAttributeOperation.TreetableCopy,
                CopyAttributeOperation.Preview));

        tempMap.put(TABLE_DESCRIPTION_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.Flatten,
                CopyAttributeOperation.Sort,
                CopyAttributeOperation.Filter,
                CopyAttributeOperation.Reverse,
                CopyAttributeOperation.ByExternal,
                CopyAttributeOperation.Coalesce,
                CopyAttributeOperation.Treetable,
                CopyAttributeOperation.TreetableCopy,
                CopyAttributeOperation.Preview));

        tempMap.put(SNAPSHOT_VIEWPORT_TYPE, EnumSet.allOf(CopyAttributeOperation.class));

        tempMap.put(ADD_ONLY_TABLE_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.DropColumns,
                CopyAttributeOperation.UpdateView,
                CopyAttributeOperation.View,
                CopyAttributeOperation.ByExternal,
                CopyAttributeOperation.Coalesce));

        tempMap.put(PREDEFINED_ROLLUP_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.Flatten,
                CopyAttributeOperation.Sort,
                CopyAttributeOperation.Filter,
                CopyAttributeOperation.Reverse,
                CopyAttributeOperation.Coalesce));

        tempMap.put(PREVIEW_PARENT_TABLE, EnumSet.of(CopyAttributeOperation.Flatten));

        // Key column and unique keys attributes
        final EnumSet<CopyAttributeOperation> uniqueKeysCopyAttributes = EnumSet.copyOf(LEGACY_COPY_ATTRIBUTES);
        uniqueKeysCopyAttributes.add(CopyAttributeOperation.Reverse);
        uniqueKeysCopyAttributes.add(CopyAttributeOperation.WouldMatch);
        tempMap.put(UNIQUE_KEYS_ATTRIBUTE, uniqueKeysCopyAttributes);
        tempMap.put(KEY_COLUMNS_ATTRIBUTE, uniqueKeysCopyAttributes);

        tempMap.put(PLUGIN_NAME, EnumSet.of(
                CopyAttributeOperation.Coalesce,
                CopyAttributeOperation.Filter,
                CopyAttributeOperation.Sort,
                CopyAttributeOperation.Reverse,
                CopyAttributeOperation.Flatten,
                CopyAttributeOperation.LastBy,
                CopyAttributeOperation.FirstBy,
                CopyAttributeOperation.ByExternal,
                CopyAttributeOperation.Preview));

        tempMap.put(SORTED_COLUMNS_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.Flatten,
                CopyAttributeOperation.Filter,
                CopyAttributeOperation.ByExternal));

        tempMap.put(STREAM_TABLE_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.Coalesce,
                CopyAttributeOperation.Filter,
                CopyAttributeOperation.Sort,
                CopyAttributeOperation.Reverse,
                CopyAttributeOperation.Flatten,
                CopyAttributeOperation.ByExternal,
                CopyAttributeOperation.Preview,
                CopyAttributeOperation.View, // and Select, if added
                CopyAttributeOperation.UpdateView, // and Update, if added
                CopyAttributeOperation.DropColumns,
                CopyAttributeOperation.Join,
                CopyAttributeOperation.WouldMatch));

        attributeToCopySet = Collections.unmodifiableMap(tempMap);
    }

    static protected boolean shouldCopyAttribute(String attrName, CopyAttributeOperation copyType) {
        return attributeToCopySet.getOrDefault(attrName, LEGACY_COPY_ATTRIBUTES).contains(copyType);
    }

    /**
     * Copy this table's attributes to the specified table. Attributes will be copied based upon the input
     * {@link CopyAttributeOperation}.
     *
     * @param dest The table to copy attributes to
     * @param copyType The operation being performed that requires attributes to be copied.
     */
    public void copyAttributes(Table dest, CopyAttributeOperation copyType) {
        copyAttributes(this, dest, copyType);
    }

    /**
     * Copy this table's attributes to the specified table. Attributes are copied based on a predicate.
     *
     * @param dest The table to copy attributes to
     * @param shouldCopy should we copy this attribute?
     */
    public void copyAttributes(Table dest, Predicate<String> shouldCopy) {
        copyAttributes(this, dest, shouldCopy);
    }

    /**
     * Copy attributes between tables. Attributes will be copied based upon the input {@link CopyAttributeOperation}.
     *
     * @param dest The table to copy attributes to
     * @param copyType The operation being performed that requires attributes to be copied.
     */
    static void copyAttributes(Table source, Table dest, CopyAttributeOperation copyType) {
        copyAttributes(source, dest, attrName -> shouldCopyAttribute(attrName, copyType));
    }

    /**
     * Copy attributes between tables. Attributes are copied based on a predicate.
     *
     * @param source The table to copy attributes from
     * @param dest The table to copy attributes to
     * @param shouldCopy should we copy this attribute?
     */
    private static void copyAttributes(Table source, Table dest, Predicate<String> shouldCopy) {
        for (final Map.Entry<String, Object> attrEntry : source.getAttributes().entrySet()) {
            final String attrName = attrEntry.getKey();
            if (shouldCopy.test(attrName)) {
                dest.setAttribute(attrName, attrEntry.getValue());
            }
        }
    }

    /**
     * Returns true if this table is static, or has an attribute asserting that no modifies, shifts, or removals are
     * generated.
     *
     * @return true if this table does not produce modifications, shifts, or removals
     */
    public boolean isAddOnly() {
        if (!isLive()) {
            return true;
        }
        return Boolean.TRUE.equals(getAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE));
    }

    /**
     * Returns true if this table is a stream table.
     *
     * @return Whether this table is a stream table
     * @see #STREAM_TABLE_ATTRIBUTE
     */
    public boolean isStream() {
        return StreamTableTools.isStream(this);
    }


    @Override
    public Table dropStream() {
        if (!isStream()) {
            return this;
        }
        final Table result = copy();
        result.setAttribute(STREAM_TABLE_ATTRIBUTE, false);
        return result;
    }

    // ------------------------------------------------------------------------------------------------------------------
    // DynamicTable Implementation
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    public final void addParentReference(@NotNull final Object parent) {
        if (DynamicNode.notDynamicOrIsRefreshing(parent)) {
            setRefreshing(true);
            parents.add(parent);
            if (parent instanceof LivenessReferent) {
                manage((LivenessReferent) parent);
            }
        }
    }

    @Override
    public boolean satisfied(final long step) {
        if (lastSatisfiedStep == step) {
            return true;
        }

        // noinspection SynchronizeOnNonFinalField
        synchronized (parents) {
            // If we have no parents whatsoever then we are a source, and have no dependency chain other than the LTM
            // itself
            if (parents.isEmpty()) {
                if (LiveTableMonitor.DEFAULT.satisfied(step)) {
                    LiveTableMonitor.DEFAULT.logDependencies().append("Root node satisfied ").append(this).endl();
                    return true;
                }
                return false;
            }

            for (Object parent : parents) {
                if (parent instanceof NotificationQueue.Dependency) {
                    if (!((NotificationQueue.Dependency) parent).satisfied(step)) {
                        LiveTableMonitor.DEFAULT.logDependencies().append("Parents dependencies not satisfied for ")
                                .append(this).append(", parent=").append((NotificationQueue.Dependency) parent).endl();
                        return false;
                    }
                }
            }
        }

        LiveTableMonitor.DEFAULT.logDependencies().append("All parents dependencies satisfied ").append(this).endl();

        lastSatisfiedStep = step;

        return true;
    }

    @Override
    public void awaitUpdate() throws InterruptedException {
        LiveTableMonitor.DEFAULT.exclusiveLock().doLocked(liveTableMonitorCondition::await);
    }

    @Override
    public boolean awaitUpdate(long timeout) throws InterruptedException {
        final MutableBoolean result = new MutableBoolean(false);
        LiveTableMonitor.DEFAULT.exclusiveLock()
                .doLocked(() -> result.setValue(liveTableMonitorCondition.await(timeout, TimeUnit.MILLISECONDS)));

        return result.booleanValue();
    }

    @Override
    public void listenForUpdates(final Listener listener, final boolean replayInitialImage) {
        if (isFailed) {
            throw new IllegalStateException("Can not listen to failed table " + description);
        }
        if (isRefreshing()) {
            childListenerReferences.add(listener);
        }
        if (replayInitialImage && getIndex().nonempty()) {
            listener.setInitialImage(getIndex());
            listener.onUpdate(getIndex(), Index.FACTORY.getEmptyIndex(), Index.FACTORY.getEmptyIndex());
        }
    }

    @Override
    public void listenForUpdates(final ShiftAwareListener listener) {
        if (isFailed) {
            throw new IllegalStateException("Can not listen to failed table " + description);
        }
        if (isRefreshing()) {
            childShiftAwareListenerReferences.add(listener);
        }
    }

    @Override
    public void listenForDirectUpdates(final Listener listener) {
        if (isFailed) {
            throw new IllegalStateException("Can not listen to failed table " + description);
        }
        if (isRefreshing()) {
            directChildListenerReferences.add(listener);
        }
    }

    @Override
    public void removeUpdateListener(final Listener listenerToRemove) {
        childListenerReferences.remove(listenerToRemove);
    }

    @Override
    public void removeUpdateListener(final ShiftAwareListener listenerToRemove) {
        childShiftAwareListenerReferences.remove(listenerToRemove);
    }

    @Override
    public void removeDirectUpdateListener(final Listener listenerToRemove) {
        directChildListenerReferences.remove(listenerToRemove);
    }

    @Override
    public final void notifyListenersOnError(final Throwable e,
            @Nullable final UpdatePerformanceTracker.Entry sourceEntry) {
        isFailed = true;
        LiveTableMonitor.DEFAULT.requestSignal(liveTableMonitorCondition);

        // Notify Legacy Listeners
        directChildListenerReferences.forEach((listenerRef, listener) -> {
            // Missing async error handler invocation, and sourceEntry assignment in some cases.
            listener.onFailure(e, sourceEntry);
        });

        lastNotificationStep = LogicalClock.DEFAULT.currentStep();

        childListenerReferences.forEach((listenerRef, listener) -> LiveTableMonitor.DEFAULT
                .addNotification(listener.getErrorNotification(e, sourceEntry)));

        // Notify ShiftAwareListeners
        childShiftAwareListenerReferences.forEach((listenerRef, listener) -> LiveTableMonitor.DEFAULT
                .addNotification(listener.getErrorNotification(e, sourceEntry)));
    }

    @Override
    public final boolean isRefreshing() {
        return refreshing;
    }

    @Override
    public final boolean setRefreshing(boolean refreshing) {
        return this.refreshing = refreshing;
    }

    @Override
    public boolean isFailed() {
        return isFailed;
    }

    public boolean hasListeners() {
        return !childListenerReferences.isEmpty() || !directChildListenerReferences.isEmpty()
                || !childShiftAwareListenerReferences.isEmpty();
    }

    @Override
    public final void notifyListeners(final ShiftAwareListener.Update update) {
        Assert.eqTrue(update.valid(), "update.valid()");
        if (update.empty()) {
            update.release();
            return;
        }

        LiveTableMonitor.DEFAULT.requestSignal(liveTableMonitorCondition);

        final boolean hasNoListeners = !hasListeners();
        if (hasNoListeners) {
            final long currentStep = LogicalClock.DEFAULT.currentStep();
            Assert.lt(lastNotificationStep, "lastNotificationStep", currentStep, "LogicalClock.DEFAULT.currentStep()");
            lastNotificationStep = currentStep;
            update.release();
            return;
        }

        Assert.neqNull(update.added, "added");
        Assert.neqNull(update.removed, "removed");
        Assert.neqNull(update.modified, "modified");
        Assert.neqNull(update.shifted, "shifted");

        if (isFlat()) {
            Assert.assertion(getIndex().isFlat(), "getIndex().isFlat()", getIndex(), "getIndex()");
        }
        if (isAddOnly()) {
            Assert.assertion(update.removed.empty(), "update.removed.empty()");
            Assert.assertion(update.modified.empty(), "update.modified.empty()");
            Assert.assertion(update.shifted.empty(), "update.shifted.empty()");
        }

        // First validate that each index is in a sane state.
        if (VALIDATE_UPDATE_INDICES) {
            update.added.validate();
            update.removed.validate();
            update.modified.validate();
            update.shifted.validate();
            Assert.eq(update.modified.empty(), "update.modified.empty()", update.modifiedColumnSet.empty(),
                    "update.modifiedColumnSet.empty()");
        }

        if (VALIDATE_UPDATE_OVERLAPS) {
            validateUpdateOverlaps(update);
        }

        // Expand if we are testing or have children listening using old Listener API.
        final boolean childNeedsExpansion =
                !directChildListenerReferences.isEmpty() || !childListenerReferences.isEmpty();
        final IndexShiftDataExpander shiftExpander = childNeedsExpansion
                ? new IndexShiftDataExpander(update, getIndex())
                : IndexShiftDataExpander.EMPTY;

        if (childNeedsExpansion && VALIDATE_UPDATE_OVERLAPS) {
            // Check that expansion is valid w.r.t. historical expectations.
            shiftExpander.validate(getIndex());
        }

        // tables may only be updated once per cycle
        final long currentStep = LogicalClock.DEFAULT.currentStep();
        Assert.lt(lastNotificationStep, "lastNotificationStep", currentStep, "LogicalClock.DEFAULT.currentStep()");

        lastNotificationStep = currentStep;

        // notify direct children
        directChildListenerReferences.forEach((listenerRef, listener) -> listener.onUpdate(shiftExpander.getAdded(),
                shiftExpander.getRemoved(), shiftExpander.getModified()));

        // notify non-direct children
        final NotificationQueue notificationQueue = getNotificationQueue();
        childListenerReferences.forEach((listenerRef, listener) -> {
            final NotificationQueue.IndexUpdateNotification notification =
                    listener.getNotification(shiftExpander.getAdded(), shiftExpander.getRemoved(),
                            shiftExpander.getModified());
            notificationQueue.addNotification(notification);
        });
        childShiftAwareListenerReferences.forEach((listenerRef, listener) -> {
            final NotificationQueue.IndexUpdateNotification notification = listener.getNotification(update);
            notificationQueue.addNotification(notification);
        });

        // eventually clean up shiftExpander's data.
        if (childNeedsExpansion) {
            notificationQueue.addNotification(new TerminalNotification() {
                @Override
                public void run() {
                    shiftExpander.close();
                }
            });
        }

        update.release();
    }

    private void validateUpdateOverlaps(final ShiftAwareListener.Update update) {
        final boolean currentMissingAdds = !update.added.subsetOf(getIndex());
        final boolean currentMissingModifications = !update.modified.subsetOf(getIndex());
        final boolean previousMissingRemovals;
        try (final ReadOnlyIndex prevIndex = getIndex().getPrevIndex()) {
            previousMissingRemovals = !update.removed.subsetOf(prevIndex);
        }
        final boolean currentContainsRemovals;
        try (final ReadOnlyIndex removedMinusAdded = update.removed.minus(update.added)) {
            currentContainsRemovals = removedMinusAdded.overlaps(getIndex());
        }

        if (!previousMissingRemovals && !currentMissingAdds && !currentMissingModifications &&
                (!currentContainsRemovals || !update.shifted.empty())) {
            return;
        }

        // Excuse the sloppiness in Index closing after this point, we're planning to crash the process anyway...

        String serializedIndices = null;
        if (PRINT_SERIALIZED_UPDATE_OVERLAPS) {
            // The indices are really rather complicated, if we fail this check let's generate a serialized
            // representation
            // of them that can later be loaded into a debugger. If this fails, we'll ignore it and continue with our
            // regularly scheduled exception.
            try {
                final StringBuilder outputBuffer = new StringBuilder();
                final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                final ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);

                final BiConsumer<String, Object> append = (name, obj) -> {
                    try {
                        objectOutputStream.writeObject(obj);
                        outputBuffer.append(name);
                        outputBuffer.append(Base64.byteArrayToBase64(byteArrayOutputStream.toByteArray()));
                        byteArrayOutputStream.reset();
                        objectOutputStream.reset();
                    } catch (final Exception ignored) {
                    }
                };

                append.accept("getIndex().getPrevIndex=", getIndex().getPrevIndex());
                append.accept("getIndex()=", getIndex().getPrevIndex());
                append.accept("added=", update.added);
                append.accept("removed=", update.removed);
                append.accept("modified=", update.modified);
                append.accept("shifted=", update.shifted);

                serializedIndices = outputBuffer.toString();
            } catch (final Exception ignored) {
            }
        }

        // If we're still here, we know that things are off the rails, and we want to fire the assertion
        final Index removalsMinusPrevious = update.removed.minus(getIndex().getPrevIndex());
        final Index addedMinusCurrent = update.added.minus(getIndex());
        final Index removedIntersectCurrent = update.removed.intersect(getIndex());
        final Index modifiedMinusCurrent = update.modified.minus(getIndex());

        // Everything is messed up for this table, print out the indices in an easy to understand way
        final LogOutput logOutput = new LogOutputStringImpl()
                .append("Index update error detected: ")
                .append(LogOutput::nl).append("\t          previousIndex=").append(getIndex().getPrevIndex())
                .append(LogOutput::nl).append("\t           currentIndex=").append(getIndex())
                .append(LogOutput::nl).append("\t                  added=").append(update.added)
                .append(LogOutput::nl).append("\t                removed=").append(update.removed)
                .append(LogOutput::nl).append("\t               modified=").append(update.modified)
                .append(LogOutput::nl).append("\t                shifted=").append(update.shifted.toString())
                .append(LogOutput::nl).append("\t  removalsMinusPrevious=").append(removalsMinusPrevious)
                .append(LogOutput::nl).append("\t      addedMinusCurrent=").append(addedMinusCurrent)
                .append(LogOutput::nl).append("\t   modifiedMinusCurrent=").append(modifiedMinusCurrent);

        if (update.shifted.empty()) {
            logOutput.append(LogOutput::nl).append("\tremovedIntersectCurrent=").append(removedIntersectCurrent);
        }

        final String indexUpdateErrorMessage = logOutput.toString();

        log.error().append(indexUpdateErrorMessage).endl();

        if (serializedIndices != null) {
            log.error().append("Index update error detected: serialized data=").append(serializedIndices).endl();
        }

        Assert.assertion(false, "!(previousMissingRemovals || currentMissingAdds || " +
                "currentMissingModifications || (currentContainsRemovals && shifted.empty()))",
                indexUpdateErrorMessage);
    }

    /**
     * Get the notification queue to insert notifications into as they are generated by listeners during
     * {@link #notifyListeners(Index, Index, Index)}. This method may be overridden to provide a different notification
     * queue than the {@link LiveTableMonitor#DEFAULT} instance for more complex behavior.
     *
     * @return The {@link NotificationQueue} to add to.
     */
    protected NotificationQueue getNotificationQueue() {
        return LiveTableMonitor.DEFAULT;
    }

    @Override
    public long getLastNotificationStep() {
        return lastNotificationStep;
    }

    @Override
    public void setLastNotificationStep(long lastNotificationStep) {
        this.lastNotificationStep = lastNotificationStep;
    }

    @Override
    public boolean isSystemicObject() {
        return Boolean.TRUE.equals(getAttribute(Table.SYSTEMIC_TABLE_ATTRIBUTE));
    }

    @Override
    public void markSystemic() {
        setAttribute(Table.SYSTEMIC_TABLE_ATTRIBUTE, Boolean.TRUE);
    }

    /**
     * Simplest appropriate legacy InstrumentedListener implementation for BaseTable and descendants. It's expected that
     * most use-cases will require overriding onUpdate() - the default implementation simply passes index updates
     * through to the dependent's listeners.
     *
     * It is preferred to use {@link ShiftAwareListenerImpl} over {@link ListenerImpl}
     */
    public static class ListenerImpl extends InstrumentedListener {

        @ReferentialIntegrity
        private final DynamicTable parent;
        private final DynamicTable dependent;

        public ListenerImpl(String description, DynamicTable parent, DynamicTable dependent) {
            super(description);
            this.parent = parent;
            this.dependent = dependent;
            if (parent.isRefreshing()) {
                manage(parent);
                dependent.addParentReference(this);
            }
        }

        @Override
        public void onUpdate(Index added, Index removed, Index modified) {
            dependent.notifyListeners(new ShiftAwareListener.Update(added.clone(), removed.clone(), modified.clone(),
                    IndexShiftData.EMPTY, ModifiedColumnSet.ALL));
        }

        @Override
        public final void onFailureInternal(Throwable originalException, UpdatePerformanceTracker.Entry sourceEntry) {
            onFailureInternalWithDependent(dependent, originalException, sourceEntry);
        }

        @Override
        public boolean canExecute(final long step) {
            return parent.satisfied(step);
        }

        @Override
        protected void destroy() {
            super.destroy();
            parent.removeUpdateListener(this);
            parent.removeDirectUpdateListener(this);
        }
    }

    /**
     * Simplest appropriate InstrumentedShiftAwareListener implementation for BaseTable and descendants. It's expected
     * that most use-cases will require overriding onUpdate() - the default implementation simply passes index updates
     * through to the dependent's listeners.
     */
    public static class ShiftAwareListenerImpl extends InstrumentedShiftAwareListener {

        @ReferentialIntegrity
        private final DynamicTable parent;
        private final DynamicTable dependent;
        private final boolean canReuseModifiedColumnSet;

        public ShiftAwareListenerImpl(String description, DynamicTable parent, DynamicTable dependent) {
            super(description);
            this.parent = parent;
            this.dependent = dependent;
            if (parent.isRefreshing()) {
                manage(parent);
                dependent.addParentReference(this);
            }
            if (parent instanceof QueryTable && dependent instanceof QueryTable) {
                final QueryTable pqt = (QueryTable) parent;
                final QueryTable dqt = (QueryTable) dependent;
                canReuseModifiedColumnSet = !pqt.modifiedColumnSet.requiresTransformer(dqt.modifiedColumnSet);
            } else {
                // We cannot reuse the modifiedColumnSet since there are no assumptions that can be made w.r.t. parent's
                // and dependent's column source mappings.
                canReuseModifiedColumnSet = false;
            }
        }

        @Override
        public void onUpdate(final Update upstream) {
            final Update downstream;
            if (!canReuseModifiedColumnSet) {
                downstream = upstream.copy();
                downstream.modifiedColumnSet = ModifiedColumnSet.ALL;
            } else {
                downstream = upstream.acquire();
            }
            dependent.notifyListeners(downstream);
        }

        @Override
        public void onFailureInternal(Throwable originalException, UpdatePerformanceTracker.Entry sourceEntry) {
            onFailureInternalWithDependent(dependent, originalException, sourceEntry);
        }

        @Override
        public boolean canExecute(final long step) {
            return parent.satisfied(step);
        }

        @Override
        protected void destroy() {
            super.destroy();
            parent.removeUpdateListener(this);
        }

        protected DynamicTable getParent() {
            return parent;
        }
    }

    @Override
    public Table withKeys(String... columns) {
        if (columns == null || columns.length == 0) {
            throw new IllegalArgumentException("withKeys() must be called with at least one key column");
        }

        if (getAttribute(Table.KEY_COLUMNS_ATTRIBUTE) != null) {
            throw new IllegalStateException(
                    "This table already has a set of key columns.  You must create a new table in order to use different keys");
        }

        checkAvailableColumns(columns);

        setAttribute(Table.KEY_COLUMNS_ATTRIBUTE, StringUtils.joinStrings(columns, ","));
        return this;
    }

    @Override
    public Table withUniqueKeys(String... columns) {
        final Table t = withKeys(columns);
        setAttribute(Table.UNIQUE_KEYS_ATTRIBUTE, true);

        return t;
    }

    @Override
    public Table restrictSortTo(String... allowedSortingColumns) {
        Assert.neqNull(allowedSortingColumns, "allowedSortingColumns");

        checkAvailableColumns(allowedSortingColumns);
        setAttribute(Table.SORTABLE_COLUMNS_ATTRIBUTE, StringUtils.joinStrings(allowedSortingColumns, ","));

        return this;
    }

    @Override
    public Table clearSortingRestrictions() {
        attributes.remove(SORTABLE_COLUMNS_ATTRIBUTE);
        return this;
    }

    @Override
    public Table layoutHints(String hints) {
        setAttribute(Table.LAYOUT_HINTS_ATTRIBUTE, hints);
        return this;
    }

    private void checkAvailableColumns(String[] columns) {
        final Map<String, ? extends ColumnSource<?>> sourceMap = getColumnSourceMap();
        final String[] missingColumns =
                Arrays.stream(columns).filter(col -> !sourceMap.containsKey(col)).toArray(String[]::new);

        if (missingColumns.length > 0) {
            throw new NoSuchColumnException(sourceMap.keySet(), Arrays.asList(missingColumns));
        }
    }

    void maybeUpdateSortableColumns(Table destination) {
        final String currentSortableColumns = (String) getAttribute(SORTABLE_COLUMNS_ATTRIBUTE);
        if (currentSortableColumns == null) {
            return;
        }

        destination.restrictSortTo(Arrays.stream(currentSortableColumns.split(","))
                .filter(destination.getColumnSourceMap()::containsKey).toArray(String[]::new));
    }

    void maybeUpdateSortableColumns(Table destination, MatchPair[] renamedColumns) {
        final String currentSortableColumns = (String) getAttribute(SORTABLE_COLUMNS_ATTRIBUTE);
        if (currentSortableColumns == null) {
            return;
        }

        final BiMap<String, String> columnMapping = HashBiMap.create();

        // Create a bi-directional map of New -> Old column name so we can see if
        // a) A column that was sortable in the old table has been renamed & we should make the new column sortable
        // b) The original column exists, and has not been replaced by another. For example
        // T1 = [ Col1, Col2, Col3 ]; T1.renameColumns(Col1=Col3, Col2];
        if (renamedColumns != null) {
            for (MatchPair mp : renamedColumns) {
                // Only the last grouping matters.
                columnMapping.forcePut(mp.left(), mp.right());
            }
        }

        final Set<String> sortableColumns = new HashSet<>();

        // Process the original set of sortable columns, adding them to the new set if one of the below
        // 1) The column exists in the new table and was not renamed in any way but the Identity (C1 = C1)
        // 2) The column does not exist in the new table, but was renamed to another (C2 = C1)
        final Map<String, ? extends ColumnSource<?>> sourceMap = destination.getColumnSourceMap();
        for (String col : currentSortableColumns.split(",")) {
            // Only add it to the set of sortable columns if it hasn't changed in an unknown way
            final String maybeRenamedColumn = columnMapping.get(col);
            if (sourceMap.get(col) != null && (maybeRenamedColumn == null || maybeRenamedColumn.equals(col))) {
                sortableColumns.add(col);
            } else {
                final String newName = columnMapping.inverse().get(col);
                if (newName != null) {
                    sortableColumns.add(newName);
                }
            }
        }

        // Apply the new mapping to the result table.
        destination.restrictSortTo(sortableColumns.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
    }

    void maybeUpdateSortableColumns(Table destination, SelectColumn[] selectCols) {
        final String currentSortableColumns = (String) getAttribute(SORTABLE_COLUMNS_ATTRIBUTE);
        if (currentSortableColumns == null) {
            return;
        }

        final Set<String> currentSortableSet = CollectionUtil.setFromArray(currentSortableColumns.split(","));
        final Set<String> newSortableSet = new HashSet<>();

        for (SelectColumn sc : selectCols) {
            final SourceColumn realColumn;

            if (sc instanceof SourceColumn) {
                realColumn = (SourceColumn) sc;
            } else if (sc instanceof SwitchColumn && ((SwitchColumn) sc).getRealColumn() instanceof SourceColumn) {
                realColumn = (SourceColumn) ((SwitchColumn) sc).getRealColumn();
            } else {
                newSortableSet.remove(sc.getName());
                currentSortableSet.remove(sc.getName());
                continue;
            }

            if (currentSortableSet.contains(realColumn.getSourceName())) {
                newSortableSet.add(sc.getName());
            }
        }

        // Now go through the other columns in the table and add them if they were unchanged
        final Map<String, ? extends ColumnSource<?>> sourceMap = destination.getColumnSourceMap();
        for (String col : currentSortableSet) {
            if (sourceMap.containsKey(col)) {
                newSortableSet.add(col);
            }
        }

        destination.restrictSortTo(newSortableSet.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
    }

    /**
     * Check if all the specified columns are sortable.
     *
     * @throws NotSortableException If one or more of the specified columns are unsortable.
     */
    void assertSortable(String... columns) throws NotSortableException {
        // Does this table actually have the requested columns?
        checkAvailableColumns(columns);

        final String sortable = (String) getAttribute(SORTABLE_COLUMNS_ATTRIBUTE);

        if (sortable == null) {
            return;
        }

        final List<String> sortableColSet;
        if (sortable.isEmpty()) {
            sortableColSet = Collections.emptyList();
        } else {
            sortableColSet = Arrays.asList(sortable.split(","));
        }

        // TODO: This is hacky. DbSortedFilteredTableModel will update the table with __ABS__ prefixed columns
        // TODO: when the user requests to sort absolute.
        final Set<String> unsortable = Arrays.stream(columns)
                .map(cn -> cn.startsWith("__ABS__") ? cn.replace("__ABS__", "") : cn).collect(Collectors.toSet());
        sortableColSet.forEach(unsortable::remove);

        if (unsortable.isEmpty()) {
            return;
        }

        // If this is null, we never should have gotten to this point because _all_ columns are sortable.
        Assert.neqNull(sortable, "sortable");

        throw new NotSortableException(unsortable, sortableColSet);
    }

    /**
     * Copy all valid column-descriptions from this table's attributes to the destination table's attributes
     *
     * @param destination the table which shall possibly have a column-description attribute created
     */
    void maybeCopyColumnDescriptions(final Table destination) {
        // noinspection unchecked
        final Map<String, String> sourceDescriptions =
                (Map<String, String>) getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE);
        maybeCopyColumnDescriptions(destination, sourceDescriptions);
    }

    /**
     * Copy all valid column-descriptions from this table's attributes to the destination table's attributes after a
     * `renameColumns()` operation
     *
     * @param destination the table which shall possibly have a column-description attribute created
     * @param renamedColumns an array of the columns which have been renamed
     */
    void maybeCopyColumnDescriptions(final Table destination, final MatchPair[] renamedColumns) {
        // noinspection unchecked
        final Map<String, String> oldDescriptions =
                (Map<String, String>) getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE);

        if (oldDescriptions == null || oldDescriptions.isEmpty()) {
            return; // short-circuit; there are no column-descriptions in this operation
        }
        final Map<String, String> sourceDescriptions = new HashMap<>(oldDescriptions);

        if (renamedColumns != null && renamedColumns.length != 0) {
            for (final MatchPair mp : renamedColumns) {
                final String desc = sourceDescriptions.remove(mp.right());
                if (desc != null) {
                    sourceDescriptions.put(mp.left(), desc);
                }
            }
        }

        maybeCopyColumnDescriptions(destination, sourceDescriptions);
    }

    /**
     * Copy all valid column-descriptions from this table's attributes to the destination table's attributes after an
     * `update()` operation. Any column which is possibly being updated as part of this operation will have their
     * description invalidated
     *
     * @param destination the table which shall possibly have a column-description attribute created
     * @param selectColumns columns which may be changed during this operation, and have their descriptions invalidated
     */
    void maybeCopyColumnDescriptions(final Table destination, final SelectColumn[] selectColumns) {
        // noinspection unchecked
        final Map<String, String> oldDescriptions =
                (Map<String, String>) getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE);

        if (oldDescriptions == null || oldDescriptions.isEmpty()) {
            return; // short-circuit; there are no column-descriptions in this operation
        }
        final Map<String, String> sourceDescriptions = new HashMap<>(oldDescriptions);

        if (selectColumns != null && selectColumns.length != 0) {
            for (final SelectColumn sc : selectColumns) {
                sourceDescriptions.remove(sc.getName());
            }
        }

        maybeCopyColumnDescriptions(destination, sourceDescriptions);
    }

    /**
     * Copy all valid column-descriptions from this table's attributes to the destination table's attributes after a
     * `join()` operation. The left-table descriptions will be left as-is, and the added columns from the right-table
     * will be added to the destination-table. Joining column-descriptions will come from the right-table IFF there is
     * no description for the column on the left-table
     *
     * @param destination the table which shall possibly have a column-description attribute created
     * @param rightTable the right-side table, from where column-descriptions may be copied
     * @param joinedColumns the columns on which this table is being joined
     * @param addColumns the right-table's columns which are being added by the join operation
     */
    void maybeCopyColumnDescriptions(final Table destination, final Table rightTable, final MatchPair[] joinedColumns,
            final MatchPair[] addColumns) {
        // noinspection unchecked
        final Map<String, String> leftDescriptions =
                (Map<String, String>) getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE);
        // noinspection unchecked
        final Map<String, String> rightDescriptions =
                (Map<String, String>) rightTable.getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE);

        if ((leftDescriptions == null || leftDescriptions.isEmpty())
                && (rightDescriptions == null || rightDescriptions.isEmpty())) {
            return; // short-circuit; there are no column-descriptions in this operation
        }

        // start with the left-table descriptions, if any
        final Map<String, String> sourceDescriptions =
                leftDescriptions == null ? new HashMap<>() : new HashMap<>(leftDescriptions);

        // and join the right-table descriptions, if any
        if (rightDescriptions != null && !rightDescriptions.isEmpty()) {
            Stream.concat(joinedColumns == null ? Stream.empty() : Arrays.stream(joinedColumns),
                    addColumns == null ? Stream.empty() : Arrays.stream(addColumns))
                    .forEach(mp -> {
                        final String desc = rightDescriptions.get(mp.right());
                        if (desc != null) {
                            sourceDescriptions.putIfAbsent(mp.left(), desc);
                        }
                    });
        }

        maybeCopyColumnDescriptions(destination, sourceDescriptions);
    }

    /**
     * Helper-method used by other `maybeCopyColumnDescriptions(...)` methods
     *
     * @param destination the table which shall possibly have a column-description attribute created
     * @param sourceDescriptions column name->description mapping
     */
    private static void maybeCopyColumnDescriptions(final Table destination,
            final Map<String, String> sourceDescriptions) {
        if (sourceDescriptions == null || sourceDescriptions.isEmpty()) {
            return; // short-circuit; there are no column-descriptions in this operation
        }

        final Map<String, String> destDescriptions = new HashMap<>();
        for (final Map.Entry<String, String> attrEntry : sourceDescriptions.entrySet()) {
            if (destination.hasColumns(attrEntry.getKey())) {
                destDescriptions.put(attrEntry.getKey(), attrEntry.getValue());
            }
        }

        if (!destDescriptions.isEmpty()) {
            // only add if any column-descriptions have survived to this point
            destination.setAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE, destDescriptions);
        }
    }

    /**
     * Copies this table, but with a new set of attributes.
     *
     * @return an identical table; but with a new set of attributes
     */
    public Table copy() {
        return QueryPerformanceRecorder.withNugget("copy()", sizeForInstrumentation(), () -> {
            final Mutable<Table> result = new MutableObject<>();

            final ShiftAwareSwapListener swapListener = createSwapListenerIfRefreshing(ShiftAwareSwapListener::new);
            initializeWithSnapshot("copy", swapListener, (usePrev, beforeClockValue) -> {
                final QueryTable resultTable = (QueryTable) getSubTable(getIndex());
                propagateFlatness(resultTable);
                copyAttributes(resultTable, a -> true);

                if (swapListener != null) {
                    final ShiftAwareListenerImpl listener = new ShiftAwareListenerImpl("copy()", this, resultTable);
                    swapListener.setListenerAndResult(listener, resultTable);
                    resultTable.addParentReference(swapListener);
                }

                result.setValue(resultTable);

                return true;
            });

            return result.getValue();
        });
    }

    @Override
    public Table setTotalsTable(TotalsTableBuilder builder) {
        final Table result = copy();
        result.setAttribute(TOTALS_TABLE_ATTRIBUTE, builder.buildDirective());
        return result;
    }

    @Override
    public Table setColumnRenderers(ColumnRenderersBuilder builder) {
        if (builder.isEmpty()) {
            return this;
        }

        final Set<String> existingColumns = getDefinition().getColumnNames()
                .stream()
                .filter(column -> !ColumnFormattingValues.isFormattingColumn(column))
                .collect(Collectors.toSet());

        final String[] unknownColumns = builder.getColumnSet()
                .stream()
                .filter(column -> !existingColumns.contains(column))
                .toArray(String[]::new);

        if (unknownColumns.length > 0) {
            throw new RuntimeException(
                    "Unknown columns: " + Arrays.toString(unknownColumns) + ", available columns = " + existingColumns);
        }

        final Table result = copy();
        result.setAttribute(COLUMN_RENDERERS_ATTRIBUTE, builder.buildDirective());
        return result;
    }

    public <SL extends SwapListenerBase<?>> void initializeWithSnapshot(
            String logPrefix, SL swapListener, ConstructSnapshot.SnapshotFunction snapshotFunction) {
        if (swapListener == null) {
            Assert.eqFalse(isRefreshing(), "isRefreshing");
            snapshotFunction.call(false, LogicalClock.DEFAULT.currentValue());
            return;
        }
        ConstructSnapshot.callDataSnapshotFunction(logPrefix, swapListener.makeSnapshotControl(), snapshotFunction);
    }

    public interface SwapListenerFactory<T extends SwapListenerBase<?>> {
        T newListener(BaseTable sourceTable);
    }

    /**
     * If we are a refreshing table, then we should create a swap listener and listen for updates.
     *
     * Otherwise, we return null.
     *
     * @return a swap listener for this table (or null)
     */
    @Nullable
    public <T extends SwapListenerBase<?>> T createSwapListenerIfRefreshing(final SwapListenerFactory<T> factory) {
        if (!isRefreshing()) {
            return null;
        }

        final T swapListener = factory.newListener(this);
        swapListener.subscribeForUpdates();
        return swapListener;
    }

    /**
     * <p>
     * If this table is flat, then set the result table flat.
     * </p>
     *
     * <p>
     * This function is for use when the result table shares an Index; such that if this table is flat, the result table
     * must also be flat.
     * </p>
     *
     * @param result the table derived from this table
     */
    void propagateFlatness(QueryTable result) {
        if (isFlat()) {
            result.setFlat();
        }
    }

    // ------------------------------------------------------------------------------------------------------------------
    // Reference Counting
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    protected void destroy() {
        super.destroy();
        // NB: We should not assert things about empty listener lists, here, given that listener cleanup might never
        // happen or happen out of order if the listeners were GC'd and not explicitly left unmanaged.
        childListenerReferences.clear();
        directChildListenerReferences.clear();
        parents.clear();
    }

    @Override
    public Table withTableDescription(String description) {
        final Table result = copy();
        result.setAttribute(TABLE_DESCRIPTION_ATTRIBUTE, description);
        return result;
    }

    @Override
    public Table withColumnDescription(Map<String, String> descriptions) {
        if (!hasColumns(descriptions.keySet())) {
            final Map<String, ColumnDefinition<?>> existingColumns = getDefinition().getColumnNameMap();
            throw new IllegalArgumentException(
                    "Cannot set column descriptions.  The table does not contain the following columns [ " +
                            descriptions.keySet().stream()
                                    .filter(col -> !existingColumns.containsKey(col))
                                    .collect(Collectors.joining(", "))
                            + " ]");
        }

        final Table result = copy();

        // noinspection unchecked
        Map<String, String> existingDescriptions =
                (Map<String, String>) result.getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE);
        if (existingDescriptions == null) {
            existingDescriptions = new HashMap<>();
            result.setAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE, existingDescriptions);
        }

        existingDescriptions.putAll(descriptions);
        return result;
    }
}
