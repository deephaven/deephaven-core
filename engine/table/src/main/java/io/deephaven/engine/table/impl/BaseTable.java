/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.deephaven.base.Base64;
import io.deephaven.base.StringUtils;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.reference.SimpleReference;
import io.deephaven.base.reference.WeakSimpleReference;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.exceptions.NotSortableException;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.SourceColumn;
import io.deephaven.engine.table.impl.select.SwitchColumn;
import io.deephaven.engine.table.impl.util.FieldUtils;
import io.deephaven.engine.updategraph.*;
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker;
import io.deephaven.hash.KeyedObjectHashSet;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.annotations.ReferentialIntegrity;
import io.deephaven.util.datastructures.SimpleReferenceManager;
import io.deephaven.util.datastructures.hash.IdentityKeyedObjectKey;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Condition;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base abstract class all standard table implementations.
 */
public abstract class BaseTable extends BaseGridAttributes<Table, QueryTable>
        implements TableDefaults, NotificationStepReceiver, NotificationStepSource {

    private static final long serialVersionUID = 1L;

    private static final boolean VALIDATE_UPDATE_INDICES =
            Configuration.getInstance().getBooleanWithDefault("BaseTable.validateUpdateIndices", false);
    public static final boolean VALIDATE_UPDATE_OVERLAPS =
            Configuration.getInstance().getBooleanWithDefault("BaseTable.validateUpdateOverlaps", true);
    public static final boolean PRINT_SERIALIZED_UPDATE_OVERLAPS =
            Configuration.getInstance().getBooleanWithDefault("BaseTable.printSerializedUpdateOverlaps", false);

    private static final Logger log = LoggerFactory.getLogger(BaseTable.class);

    private static final AtomicReferenceFieldUpdater<BaseTable, Condition> CONDITION_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(BaseTable.class, Condition.class, "updateGraphProcessorCondition");

    private static final AtomicReferenceFieldUpdater<BaseTable, Collection> PARENTS_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(BaseTable.class, Collection.class, "parents");
    private static final Collection<Object> EMPTY_PARENTS = Collections.emptyList();

    private static final AtomicReferenceFieldUpdater<BaseTable, SimpleReferenceManager> CHILD_LISTENER_REFERENCES_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(
                    BaseTable.class, SimpleReferenceManager.class, "childListenerReferences");
    private static final SimpleReferenceManager<TableUpdateListener, ? extends SimpleReference<TableUpdateListener>> EMPTY_CHILD_LISTENER_REFERENCES =
            new SimpleReferenceManager<>((final TableUpdateListener listener) -> {
                throw new UnsupportedOperationException("EMPTY_CHILDREN does not support adds");
            }, Collections.emptyList());

    /**
     * This table's definition.
     */
    protected final TableDefinition definition;

    /**
     * This table's description.
     */
    protected final String description;

    // Fields for DynamicNode implementation and update propagation support
    private volatile boolean refreshing;
    @SuppressWarnings({"FieldMayBeFinal", "unused"}) // Set via ensureField with CONDITION_UPDATER
    private volatile Condition updateGraphProcessorCondition;
    @SuppressWarnings("FieldMayBeFinal") // Set via ensureField with PARENTS_UPDATER
    private volatile Collection<Object> parents = EMPTY_PARENTS;
    @SuppressWarnings("FieldMayBeFinal") // Set via ensureField with CHILD_LISTENER_REFERENCES_UPDATER
    private volatile SimpleReferenceManager<TableUpdateListener, ? extends SimpleReference<TableUpdateListener>> childListenerReferences =
            EMPTY_CHILD_LISTENER_REFERENCES;
    private volatile long lastNotificationStep;
    private volatile long lastSatisfiedStep;
    private volatile boolean isFailed;

    /**
     * @param definition The definition for this table
     * @param description A description of this table
     * @param attributes The attributes map to use, or else {@code null} to allocate a new one
     */
    public BaseTable(
            @NotNull final TableDefinition definition,
            @NotNull final String description,
            @Nullable final Map<String, Object> attributes) {
        super(attributes);
        this.definition = definition;
        this.description = description;
        lastNotificationStep = LogicalClock.DEFAULT.currentStep();

        // Properly flag this table as systemic or not. Note that we use the initial attributes map, rather than
        // getAttribute, in order to avoid triggering the "immutable after first access" restrictions of
        // LiveAttributeMap.
        if (SystemicObjectTracker.isSystemicThread()
                && (attributes == null || !Boolean.TRUE.equals(attributes.get(Table.SYSTEMIC_TABLE_ATTRIBUTE)))) {
            setAttribute(Table.SYSTEMIC_TABLE_ATTRIBUTE, Boolean.TRUE);
        }
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

    public enum CopyAttributeOperation {
        // Do not copy any attributes
        None,

        // legacy attributes
        Flatten, Sort, UpdateView, Join, Filter,
        // new attributes
        DropColumns, View, Reverse,
        /**
         * The result tables that go in a PartitionBy TableMap
         */
        PartitionBy, Coalesce, WouldMatch, LastBy, FirstBy,

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

        // partitionBy was creating the sub table with a bespoke ACL copy; we should copy ACLs there in addition to the
        // legacy attributes
        final EnumSet<CopyAttributeOperation> aclCopyAttributes = EnumSet.copyOf(LEGACY_COPY_ATTRIBUTES);
        aclCopyAttributes.addAll(EnumSet.of(
                CopyAttributeOperation.FirstBy,
                CopyAttributeOperation.LastBy,
                CopyAttributeOperation.PartitionBy,
                CopyAttributeOperation.WouldMatch));

        // for a merged table, we'll allow operations that keep our RowSet + column sources the same to break us down
        // into constituent tables
        tempMap.put(MERGED_TABLE_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.DropColumns,
                CopyAttributeOperation.View));

        tempMap.put(INITIALLY_EMPTY_COALESCED_SOURCE_TABLE_ATTRIBUTE, EnumSet.complementOf(EnumSet.of(
                CopyAttributeOperation.Rollup,
                CopyAttributeOperation.Treetable)));
        tempMap.put(SORTABLE_COLUMNS_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.Flatten,
                CopyAttributeOperation.Sort,
                CopyAttributeOperation.Join,
                CopyAttributeOperation.Filter,
                CopyAttributeOperation.Reverse,
                CopyAttributeOperation.Coalesce,
                CopyAttributeOperation.PartitionBy,
                CopyAttributeOperation.WouldMatch,
                CopyAttributeOperation.Preview));

        tempMap.put(FILTERABLE_COLUMNS_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.Flatten,
                CopyAttributeOperation.Sort,
                CopyAttributeOperation.Join,
                CopyAttributeOperation.Filter,
                CopyAttributeOperation.Reverse,
                CopyAttributeOperation.Coalesce,
                CopyAttributeOperation.PartitionBy,
                CopyAttributeOperation.WouldMatch,
                CopyAttributeOperation.Preview));

        tempMap.put(LAYOUT_HINTS_ATTRIBUTE, EnumSet.allOf(CopyAttributeOperation.class));

        tempMap.put(TOTALS_TABLE_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.Flatten,
                CopyAttributeOperation.Sort,
                CopyAttributeOperation.Filter,
                CopyAttributeOperation.Reverse,
                CopyAttributeOperation.PartitionBy,
                CopyAttributeOperation.Coalesce));

        tempMap.put(SYSTEMIC_TABLE_ATTRIBUTE, EnumSet.of(CopyAttributeOperation.None));

        // Tree table attributes
        tempMap.put(HIERARCHICAL_CHILDREN_TABLE_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.DropColumns,
                CopyAttributeOperation.Sort,
                CopyAttributeOperation.Filter,
                CopyAttributeOperation.Reverse,
                CopyAttributeOperation.Coalesce,
                CopyAttributeOperation.UpdateView,
                CopyAttributeOperation.PartitionBy,
                CopyAttributeOperation.Flatten,
                CopyAttributeOperation.RollupCopy));

        tempMap.put(ROLLUP_LEAF_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.DropColumns,
                CopyAttributeOperation.Sort,
                CopyAttributeOperation.Filter,
                CopyAttributeOperation.Reverse,
                CopyAttributeOperation.Coalesce,
                CopyAttributeOperation.UpdateView,
                CopyAttributeOperation.PartitionBy,
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
                CopyAttributeOperation.PartitionBy,
                CopyAttributeOperation.Coalesce,
                CopyAttributeOperation.FirstBy,
                CopyAttributeOperation.LastBy,
                CopyAttributeOperation.Treetable,
                CopyAttributeOperation.TreetableCopy,
                CopyAttributeOperation.Preview));

        tempMap.put(DESCRIPTION_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.Flatten,
                CopyAttributeOperation.Sort,
                CopyAttributeOperation.Filter,
                CopyAttributeOperation.Reverse,
                CopyAttributeOperation.PartitionBy,
                CopyAttributeOperation.Coalesce,
                CopyAttributeOperation.Treetable,
                CopyAttributeOperation.TreetableCopy,
                CopyAttributeOperation.Preview));

        tempMap.put(SNAPSHOT_VIEWPORT_TYPE, EnumSet.allOf(CopyAttributeOperation.class));

        tempMap.put(ADD_ONLY_TABLE_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.DropColumns,
                CopyAttributeOperation.UpdateView,
                CopyAttributeOperation.View,
                CopyAttributeOperation.PartitionBy,
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
                CopyAttributeOperation.PartitionBy,
                CopyAttributeOperation.Preview));

        tempMap.put(SORTED_COLUMNS_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.Flatten,
                CopyAttributeOperation.Filter,
                CopyAttributeOperation.PartitionBy));

        tempMap.put(STREAM_TABLE_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.Coalesce,
                CopyAttributeOperation.Filter,
                CopyAttributeOperation.Sort,
                CopyAttributeOperation.Reverse,
                CopyAttributeOperation.Flatten,
                CopyAttributeOperation.PartitionBy,
                CopyAttributeOperation.Preview,
                CopyAttributeOperation.View, // and Select, if added
                CopyAttributeOperation.UpdateView, // and Update, if added
                CopyAttributeOperation.DropColumns,
                CopyAttributeOperation.Join,
                CopyAttributeOperation.WouldMatch));

        tempMap.put(BARRAGE_PERFORMANCE_KEY_ATTRIBUTE, EnumSet.of(
                CopyAttributeOperation.Flatten, // add flatten for now because web flattens all views
                CopyAttributeOperation.Preview));

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
    public void copyAttributes(QueryTable dest, CopyAttributeOperation copyType) {
        copyAttributes(this, dest, copyType);
    }

    /**
     * Copy this table's attributes to the specified table. Attributes are copied based on a predicate.
     *
     * @param dest The table to copy attributes to
     * @param shouldCopy should we copy this attribute?
     */
    public void copyAttributes(QueryTable dest, Predicate<String> shouldCopy) {
        copyAttributes(this, dest, shouldCopy);
    }

    /**
     * Copy attributes between tables. Attributes will be copied based upon the input {@link CopyAttributeOperation}.
     *
     * @param dest The table to copy attributes to
     * @param copyType The operation being performed that requires attributes to be copied.
     */
    static void copyAttributes(Table source, QueryTable dest, CopyAttributeOperation copyType) {
        copyAttributes(source, dest, attrName -> shouldCopyAttribute(attrName, copyType));
    }

    /**
     * Copy attributes between tables. Attributes are copied based on a predicate.
     *
     * @param source The table to copy attributes from
     * @param dest The table to copy attributes to
     * @param shouldCopy should we copy this attribute?
     */
    private static void copyAttributes(Table source, QueryTable dest, Predicate<String> shouldCopy) {
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
        if (!isRefreshing()) {
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
        return withoutAttributes(Set.of(STREAM_TABLE_ATTRIBUTE));
    }

    // ------------------------------------------------------------------------------------------------------------------
    // Implementation for update propagation support
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    public final void addParentReference(@NotNull final Object parent) {
        if (DynamicNode.notDynamicOrIsRefreshing(parent)) {
            setRefreshing(true);
            ensureParents().add(parent);
            if (parent instanceof LivenessReferent) {
                manage((LivenessReferent) parent);
            }
        }
    }

    private Collection<Object> ensureParents() {
        // noinspection unchecked
        return FieldUtils.ensureField(this, PARENTS_UPDATER, EMPTY_PARENTS,
                () -> new KeyedObjectHashSet<>(IdentityKeyedObjectKey.getInstance()));
    }

    @Override
    public boolean satisfied(final long step) {
        if (!isRefreshing() || lastSatisfiedStep == step) {
            return true;
        }

        final Collection<Object> localParents = parents;
        // If we have no parents whatsoever then we are a source, and have no dependency chain other than the UGP
        // itself
        if (localParents.isEmpty()) {
            if (UpdateGraphProcessor.DEFAULT.satisfied(step)) {
                UpdateGraphProcessor.DEFAULT.logDependencies().append("Root node satisfied ").append(this).endl();
                return true;
            }
            return false;
        }

        // noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (localParents) {
            for (Object parent : localParents) {
                if (parent instanceof NotificationQueue.Dependency) {
                    if (!((NotificationQueue.Dependency) parent).satisfied(step)) {
                        UpdateGraphProcessor.DEFAULT.logDependencies()
                                .append("Parents dependencies not satisfied for ").append(this)
                                .append(", parent=").append((NotificationQueue.Dependency) parent)
                                .endl();
                        return false;
                    }
                }
            }
        }

        UpdateGraphProcessor.DEFAULT.logDependencies()
                .append("All parents dependencies satisfied for ").append(this)
                .endl();

        lastSatisfiedStep = step;

        return true;
    }

    @Override
    public void awaitUpdate() throws InterruptedException {
        UpdateGraphProcessor.DEFAULT.exclusiveLock().doLocked(ensureCondition()::await);
    }

    @Override
    public boolean awaitUpdate(long timeout) throws InterruptedException {
        final MutableBoolean result = new MutableBoolean(false);
        UpdateGraphProcessor.DEFAULT.exclusiveLock()
                .doLocked(() -> result.setValue(ensureCondition().await(timeout, TimeUnit.MILLISECONDS)));

        return result.booleanValue();
    }

    private Condition ensureCondition() {
        return FieldUtils.ensureField(this, CONDITION_UPDATER, null, () -> UpdateGraphProcessor.DEFAULT.exclusiveLock().newCondition());
    }

    private void maybeSignal() {
        final Condition localCondition = updateGraphProcessorCondition;
        if (localCondition != null) {
            UpdateGraphProcessor.DEFAULT.requestSignal(localCondition);
        }
    }

    @Override
    public void addUpdateListener(final ShiftObliviousListener listener, final boolean replayInitialImage) {
        addUpdateListener(new LegacyListenerAdapter(listener, getRowSet()));
        if (replayInitialImage) {
            if (isRefreshing()) {
                UpdateGraphProcessor.DEFAULT.checkInitiateTableOperation();
            }
            if (getRowSet().isNonempty()) {
                listener.setInitialImage(getRowSet());
                listener.onUpdate(getRowSet(), RowSetFactory.empty(), RowSetFactory.empty());
            }
        }
    }

    @Override
    public void addUpdateListener(final TableUpdateListener listener) {
        if (isFailed) {
            throw new IllegalStateException("Can not listen to failed table " + description);
        }
        if (isRefreshing()) {
            ensureChildListenerReferences().add(listener);
        }
    }

    private SimpleReferenceManager<TableUpdateListener, ? extends SimpleReference<TableUpdateListener>> ensureChildListenerReferences() {
        // noinspection unchecked
        return FieldUtils.ensureField(this, CHILD_LISTENER_REFERENCES_UPDATER, EMPTY_CHILD_LISTENER_REFERENCES,
                () -> new SimpleReferenceManager<>((final TableUpdateListener tableUpdateListener) -> {
                    if (tableUpdateListener instanceof LegacyListenerAdapter) {
                        return (LegacyListenerAdapter) tableUpdateListener;
                    } else if (tableUpdateListener instanceof SwapListener) {
                        return ((SwapListener) tableUpdateListener).getReferenceForSource();
                    } else {
                        return new WeakSimpleReference<>(tableUpdateListener);
                    }
                }, true));
    }

    @Override
    public void removeUpdateListener(final ShiftObliviousListener listenerToRemove) {
        childListenerReferences
                .removeIf((final TableUpdateListener listener) -> listener instanceof LegacyListenerAdapter
                        && ((LegacyListenerAdapter) listener).matches(listenerToRemove));
    }

    @Override
    public void removeUpdateListener(final TableUpdateListener listenerToRemove) {
        childListenerReferences.remove(listenerToRemove);
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
        return !childListenerReferences.isEmpty();
    }

    /**
     * Initiate update delivery to this table's listeners by enqueueing update notifications.
     *
     * @param added Row keys added to the table
     * @param removed Row keys removed from the table
     * @param modified Row keys modified in the table
     */
    public final void notifyListeners(RowSet added, RowSet removed, RowSet modified) {
        notifyListeners(new TableUpdateImpl(
                added,
                removed,
                modified,
                RowSetShiftData.EMPTY,
                modified.isEmpty() ? ModifiedColumnSet.EMPTY : ModifiedColumnSet.ALL));
    }

    /**
     * Initiate update delivery to this table's listeners by enqueueing update notifications.
     *
     * @param update The set of table changes to propagate. The caller gives this update object away; the invocation of
     *        {@code notifyListeners} takes ownership, and will call {@code release} on it once it is not used anymore;
     *        callers should pass a {@code copy} for updates they intend to further use.
     */
    public final void notifyListeners(final TableUpdate update) {
        Assert.eqFalse(isFailed, "isFailed");
        final long currentStep = LogicalClock.DEFAULT.currentStep();
        // tables may only be updated once per cycle
        Assert.lt(lastNotificationStep, "lastNotificationStep", currentStep, "LogicalClock.DEFAULT.currentStep()");

        Assert.eqTrue(update.valid(), "update.valid()");
        if (update.empty()) {
            update.release();
            return;
        }

        maybeSignal();

        final boolean hasNoListeners = !hasListeners();
        if (hasNoListeners) {
            lastNotificationStep = currentStep;
            update.release();
            return;
        }

        Assert.neqNull(update.added(), "added");
        Assert.neqNull(update.removed(), "removed");
        Assert.neqNull(update.modified(), "modified");
        Assert.neqNull(update.shifted(), "shifted");

        if (isFlat()) {
            Assert.assertion(getRowSet().isFlat(), "build().isFlat()", getRowSet(), "build()");
        }
        if (isAddOnly()) {
            Assert.assertion(update.removed().isEmpty(), "update.removed.empty()");
            Assert.assertion(update.modified().isEmpty(), "update.modified.empty()");
            Assert.assertion(update.shifted().empty(), "update.shifted.empty()");
        }

        // First validate that each rowSet is in a sane state.
        if (VALIDATE_UPDATE_INDICES) {
            update.added().validate();
            update.removed().validate();
            update.modified().validate();
            update.shifted().validate();
            Assert.eq(update.modified().isEmpty(), "update.modified.empty()", update.modifiedColumnSet().empty(),
                    "update.modifiedColumnSet.empty()");
        }

        if (VALIDATE_UPDATE_OVERLAPS) {
            validateUpdateOverlaps(update);
        }

        lastNotificationStep = currentStep;

        // notify children
        final NotificationQueue notificationQueue = getNotificationQueue();
        childListenerReferences.forEach(
                (listenerRef, listener) -> notificationQueue.addNotification(listener.getNotification(update)));

        update.release();
    }

    private void validateUpdateOverlaps(final TableUpdate update) {
        final boolean currentMissingAdds = !update.added().subsetOf(getRowSet());
        final boolean currentMissingModifications = !update.modified().subsetOf(getRowSet());
        final boolean previousMissingRemovals;
        try (final RowSet prevIndex = getRowSet().copyPrev()) {
            previousMissingRemovals = !update.removed().subsetOf(prevIndex);
        }
        final boolean currentContainsRemovals;
        try (final RowSet removedMinusAdded = update.removed().minus(update.added())) {
            currentContainsRemovals = removedMinusAdded.overlaps(getRowSet());
        }

        if (!previousMissingRemovals && !currentMissingAdds && !currentMissingModifications &&
                (!currentContainsRemovals || !update.shifted().empty())) {
            return;
        }

        // Excuse the sloppiness in RowSet closing after this point, we're planning to crash the process anyway...

        String serializedIndices = null;
        if (PRINT_SERIALIZED_UPDATE_OVERLAPS) {
            // The indices are really rather complicated, if we fail this check let's generate a serialized
            // representation of them that can later be loaded into a debugger. If this fails, we'll ignore it and
            // continue with our regularly scheduled exception.
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

                append.accept("build().copyPrev=", getRowSet().copyPrev());
                append.accept("build()=", getRowSet().copyPrev());
                append.accept("added=", update.added());
                append.accept("removed=", update.removed());
                append.accept("modified=", update.modified());
                append.accept("shifted=", update.shifted());

                serializedIndices = outputBuffer.toString();
            } catch (final Exception ignored) {
            }
        }

        // If we're still here, we know that things are off the rails, and we want to fire the assertion
        final RowSet removalsMinusPrevious = update.removed().minus(getRowSet().copyPrev());
        final RowSet addedMinusCurrent = update.added().minus(getRowSet());
        final RowSet removedIntersectCurrent = update.removed().intersect(getRowSet());
        final RowSet modifiedMinusCurrent = update.modified().minus(getRowSet());

        // Everything is messed up for this table, print out the indices in an easy to understand way
        final LogOutput logOutput = new LogOutputStringImpl()
                .append("RowSet update error detected: ")
                .append(LogOutput::nl).append("\t          previousIndex=").append(getRowSet().copyPrev())
                .append(LogOutput::nl).append("\t           currentIndex=").append(getRowSet())
                .append(LogOutput::nl).append("\t                  added=").append(update.added())
                .append(LogOutput::nl).append("\t                removed=").append(update.removed())
                .append(LogOutput::nl).append("\t               modified=").append(update.modified())
                .append(LogOutput::nl).append("\t                shifted=").append(update.shifted().toString())
                .append(LogOutput::nl).append("\t  removalsMinusPrevious=").append(removalsMinusPrevious)
                .append(LogOutput::nl).append("\t      addedMinusCurrent=").append(addedMinusCurrent)
                .append(LogOutput::nl).append("\t   modifiedMinusCurrent=").append(modifiedMinusCurrent);

        if (update.shifted().empty()) {
            logOutput.append(LogOutput::nl).append("\tremovedIntersectCurrent=").append(removedIntersectCurrent);
        }

        final String indexUpdateErrorMessage = logOutput.toString();

        log.error().append(indexUpdateErrorMessage).endl();

        if (serializedIndices != null) {
            log.error().append("RowSet update error detected: serialized data=").append(serializedIndices).endl();
        }

        Assert.assertion(false, "!(previousMissingRemovals || currentMissingAdds || " +
                "currentMissingModifications || (currentContainsRemovals && shifted.empty()))",
                indexUpdateErrorMessage);
    }

    /**
     * Initiate failure delivery to this table's listeners by enqueueing error notifications.
     *
     * @param e error
     * @param sourceEntry performance tracking
     */
    public final void notifyListenersOnError(final Throwable e, @Nullable final TableListener.Entry sourceEntry) {
        Assert.eqFalse(isFailed, "isFailed");
        final long currentStep = LogicalClock.DEFAULT.currentStep();
        Assert.lt(lastNotificationStep, "lastNotificationStep", currentStep, "LogicalClock.DEFAULT.currentStep()");

        isFailed = true;
        maybeSignal();
        lastNotificationStep = currentStep;

        final NotificationQueue notificationQueue = getNotificationQueue();
        childListenerReferences.forEach((listenerRef, listener) -> notificationQueue
                .addNotification(listener.getErrorNotification(e, sourceEntry)));
    }

    /**
     * Get the notification queue to insert notifications into as they are generated by listeners during
     * {@link #notifyListeners} and {@link #notifyListenersOnError(Throwable, TableListener.Entry)}. This method may be
     * overridden to provide a different notification queue than the {@link UpdateGraphProcessor#DEFAULT} instance for
     * more complex behavior.
     *
     * @return The {@link NotificationQueue} to add to
     */
    protected NotificationQueue getNotificationQueue() {
        return UpdateGraphProcessor.DEFAULT;
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
    public Table markSystemic() {
        // TODO (https://github.com/deephaven/deephaven-core/issues/3003): Maybe we should be memoizing the result?
        return withAttributes(Map.of(Table.SYSTEMIC_TABLE_ATTRIBUTE, Boolean.TRUE));
    }

    /**
     * Simplest appropriate legacy ShiftObliviousInstrumentedListener implementation for BaseTable and descendants. It's
     * expected that most use-cases will require overriding onUpdate() - the default implementation simply passes rowSet
     * updates through to the dependent's listeners.
     *
     * It is preferred to use {@link ListenerImpl} over {@link ShiftObliviousListenerImpl}
     */
    public static class ShiftObliviousListenerImpl extends ShiftObliviousInstrumentedListener {

        @ReferentialIntegrity
        private final Table parent;
        private final BaseTable dependent;

        public ShiftObliviousListenerImpl(String description, Table parent, BaseTable dependent) {
            super(description);
            this.parent = parent;
            this.dependent = dependent;
            if (parent.isRefreshing()) {
                manage(parent);
                dependent.addParentReference(this);
            }
        }

        @Override
        public void onUpdate(RowSet added, RowSet removed, RowSet modified) {
            dependent.notifyListeners(new TableUpdateImpl(added.copy(), removed.copy(), modified.copy(),
                    RowSetShiftData.EMPTY, ModifiedColumnSet.ALL));
        }

        @Override
        public final void onFailureInternal(Throwable originalException, Entry sourceEntry) {
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
    }

    /**
     * Simplest appropriate InstrumentedShiftAwareListener implementation for BaseTable and descendants. It's expected
     * that most use-cases will require overriding onUpdate() - the default implementation simply passes rowSet updates
     * through to the dependent's listeners.
     */
    public static class ListenerImpl extends InstrumentedTableUpdateListener {

        @ReferentialIntegrity
        private final Table parent;
        private final BaseTable dependent;
        private final boolean canReuseModifiedColumnSet;

        public ListenerImpl(String description, Table parent, BaseTable dependent) {
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
                canReuseModifiedColumnSet =
                        !pqt.getModifiedColumnSetForUpdates().requiresTransformer(dqt.getModifiedColumnSetForUpdates());
            } else {
                // We cannot reuse the modifiedColumnSet since there are no assumptions that can be made w.r.t. parent's
                // and dependent's column source mappings.
                canReuseModifiedColumnSet = false;
            }
        }

        @Override
        public void onUpdate(final TableUpdate upstream) {
            final TableUpdate downstream;
            if (!canReuseModifiedColumnSet) {
                final TableUpdateImpl upstreamCopy = TableUpdateImpl.copy(upstream);
                upstreamCopy.modifiedColumnSet = ModifiedColumnSet.ALL;
                downstream = upstreamCopy;
            } else {
                downstream = upstream.acquire();
            }
            dependent.notifyListeners(downstream);
        }

        @Override
        public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
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

        protected Table getParent() {
            return parent;
        }

        protected BaseTable getDependent() {
            return dependent;
        }
    }

    @Override
    public Table withKeys(String... columns) {
        return withAttributes(Map.of(Table.KEY_COLUMNS_ATTRIBUTE, formatKeyColumns(columns)));
    }

    @Override
    public Table withUniqueKeys(String... columns) {
        return withAttributes(Map.of(
                Table.KEY_COLUMNS_ATTRIBUTE, formatKeyColumns(columns),
                Table.UNIQUE_KEYS_ATTRIBUTE, true));
    }

    private String formatKeyColumns(String... columns) {
        if (columns == null || columns.length == 0) {
            throw new IllegalArgumentException("withKeys() must be called with at least one key column");
        }
        checkAvailableColumns(Arrays.asList(columns));
        return StringUtils.joinStrings(columns, ",");
    }

    @Override
    public void checkAvailableColumns(@NotNull final Collection<String> columns) {
        final Map<String, ? extends ColumnSource<?>> sourceMap = getColumnSourceMap();
        final String[] missingColumns =
                columns.stream().filter(col -> !sourceMap.containsKey(col)).toArray(String[]::new);
        if (missingColumns.length > 0) {
            throw new NoSuchColumnException(sourceMap.keySet(), Arrays.asList(missingColumns));
        }
    }

    // TODO-RWC: Delete?
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
                columnMapping.forcePut(mp.leftColumn(), mp.rightColumn());
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
        checkAvailableColumns(Arrays.asList(columns));

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

        // TODO: This is hacky. SortTableGrpcImpl will update the table with __ABS__ prefixed columns
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
    void maybeCopyColumnDescriptions(final QueryTable destination) {
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
    void maybeCopyColumnDescriptions(final QueryTable destination, final MatchPair[] renamedColumns) {
        // noinspection unchecked
        final Map<String, String> oldDescriptions =
                (Map<String, String>) getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE);

        if (oldDescriptions == null || oldDescriptions.isEmpty()) {
            return; // short-circuit; there are no column-descriptions in this operation
        }
        final Map<String, String> sourceDescriptions = new HashMap<>(oldDescriptions);

        if (renamedColumns != null && renamedColumns.length != 0) {
            for (final MatchPair mp : renamedColumns) {
                final String desc = sourceDescriptions.remove(mp.rightColumn());
                if (desc != null) {
                    sourceDescriptions.put(mp.leftColumn(), desc);
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
    void maybeCopyColumnDescriptions(final QueryTable destination, final SelectColumn[] selectColumns) {
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
    void maybeCopyColumnDescriptions(
            final QueryTable destination,
            final Table rightTable,
            final MatchPair[] joinedColumns,
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
                        final String desc = rightDescriptions.get(mp.rightColumn());
                        if (desc != null) {
                            sourceDescriptions.putIfAbsent(mp.leftColumn(), desc);
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
    private static void maybeCopyColumnDescriptions(
            final QueryTable destination,
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
    @Override
    public QueryTable copy() {
        // TODO (https://github.com/deephaven/deephaven-core/issues/3003): Can we copy a parent to avoid lengthy chains
        // when attributes are added serially?
        return QueryPerformanceRecorder.withNugget("copy()", sizeForInstrumentation(), () -> {
            final Mutable<QueryTable> result = new MutableObject<>();

            final SwapListener swapListener = createSwapListenerIfRefreshing(SwapListener::new);
            initializeWithSnapshot("copy", swapListener, (usePrev, beforeClockValue) -> {
                final QueryTable resultTable = (QueryTable) getSubTable(getRowSet());
                propagateFlatness(resultTable);
                copyAttributes(resultTable, a -> true);

                if (swapListener != null) {
                    final ListenerImpl listener = new ListenerImpl("copy()", this, resultTable);
                    swapListener.setListenerAndResult(listener, resultTable);
                }

                result.setValue(resultTable);
                return true;
            });

            return result.getValue();
        });
    }

    @Override
    public Table setTotalsTable(String directive) {
        return withAttributes(Map.of(TOTALS_TABLE_ATTRIBUTE, directive));
    }

    public static void initializeWithSnapshot(
            String logPrefix, SwapListener swapListener, ConstructSnapshot.SnapshotFunction snapshotFunction) {
        if (swapListener == null) {
            snapshotFunction.call(false, LogicalClock.DEFAULT.currentValue());
            return;
        }
        ConstructSnapshot.callDataSnapshotFunction(logPrefix, swapListener.makeSnapshotControl(), snapshotFunction);
    }

    public interface SwapListenerFactory<T extends SwapListener> {
        T newListener(BaseTable sourceTable);
    }

    /**
     * If we are a refreshing table, then we should create a swap listener that listens for updates to this table.
     *
     * Otherwise, we return null.
     *
     * @return a swap listener for this table (or null)
     */
    @Nullable
    public <T extends SwapListener> T createSwapListenerIfRefreshing(final SwapListenerFactory<T> factory) {
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
     * This function is for use when the result table shares a RowSet; such that if this table is flat, the result table
     * must also be flat.
     * </p>
     *
     * @param result the table derived from this table
     */
    public void propagateFlatness(QueryTable result) {
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
        parents.clear();
    }
}
