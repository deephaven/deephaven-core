//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.util.SafeCloseable;

import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

/**
 * This class is used to represent a pushdown action that can be performed on a regioned column source. Note that the
 * logic to perform this action is not contained in this class.
 */
public abstract class RegionedPushdownAction {
    public static final EstimateContext DEFAULT_ESTIMATE_CONTEXT = () -> {
    };
    public static final ActionContext DEFAULT_ACTION_CONTEXT = () -> {
    };

    /**
     * A generic context for use in {@link RegionedPushdownFilterMatcher#estimatePushdownAction}
     */
    public interface EstimateContext extends SafeCloseable {
    }

    /**
     * A generic context for use in {@link RegionedPushdownFilterMatcher#performPushdownAction}
     */
    public interface ActionContext extends SafeCloseable {
    }

    // Note: the disabled static configuration values are _not_ currently final and we must test them on each call
    // to ensure that dynamic configuration changes are respected.
    protected final BooleanSupplier disabled;

    private final long filterCost;
    private final Predicate<RegionedPushdownFilterContext> contextAllows;
    private final Predicate<TableLocation> locationAllows;

    private RegionedPushdownAction(
            final BooleanSupplier disabled,
            final long filterCost,
            final Predicate<RegionedPushdownFilterContext> contextAllows,
            final Predicate<TableLocation> locationAllows) {
        this.disabled = Objects.requireNonNull(disabled);
        this.filterCost = filterCost;
        this.contextAllows = Objects.requireNonNull(contextAllows);
        this.locationAllows = Objects.requireNonNull(locationAllows);
    }

    /**
     * The cost of performing this action.
     */
    public long filterCost() {
        return filterCost;
    }

    /**
     * Determine which actions are not specifically prohibited by configuration.
     */
    public boolean allows(
            final TableLocation location,
            final RegionedPushdownFilterContext filterContext) {
        return !disabled.getAsBoolean()
                && contextAllows(filterContext)
                && locationAllows(location);
    }

    /**
     * Determine which actions are not specifically prohibited by configuration and the cost ceiling
     */
    public boolean allows(
            final TableLocation location,
            final RegionedPushdownFilterContext context,
            final long costCeiling) {
        return allows(location, context) && ceilingAllows(costCeiling);
    }

    /**
     * Determine if this action is allowed based on the cost ceiling.
     */
    protected boolean ceilingAllows(final long costCeiling) {
        return filterCost <= costCeiling;
    }

    /**
     * Determine if this action is allowed based on the current executed filter cost and the filter context predicate.
     */
    protected boolean contextAllows(final RegionedPushdownFilterContext context) {
        return context.executedFilterCost() < filterCost && contextAllows.test(context);
    }

    /**
     * Determine if this action is allowed based on the provided table location predicate.
     */
    protected boolean locationAllows(final TableLocation location) {
        return locationAllows.test(location);
    }

    /**
     * A pushdown action that is only valid when executed on a table location.
     */
    public static class Location extends RegionedPushdownAction {
        public Location(
                final BooleanSupplier disabled,
                final long filterCost,
                final Predicate<RegionedPushdownFilterContext> contextAllows,
                final Predicate<TableLocation> locationAllows) {
            super(disabled, filterCost, contextAllows, locationAllows);
        }
    }

    /**
     * A pushdown action that is only valid when executed on a column region.
     */
    public static class Region extends RegionedPushdownAction {
        private final Predicate<ColumnRegion<?>> regionAllows;

        public Region(
                final BooleanSupplier disabled,
                final long filterCost,
                final Predicate<RegionedPushdownFilterContext> contextAllows,
                final Predicate<TableLocation> locationAllows,
                final Predicate<ColumnRegion<?>> regionAllows) {
            super(disabled, filterCost, contextAllows, locationAllows);
            this.regionAllows = Objects.requireNonNull(regionAllows);
        }

        /**
         * Determine if this action is allowed for the given table location, column region, and filter context.
         */
        public boolean allows(
                final TableLocation location,
                final ColumnRegion<?> region,
                final RegionedPushdownFilterContext context) {
            return !disabled.getAsBoolean()
                    && contextAllows(context)
                    && locationAllows(location)
                    && allows(region);
        }

        /**
         * Determine if this action is allowed for the given table location, column region, filter context, and cost
         * ceiling.
         */
        public boolean allows(
                final TableLocation location,
                final ColumnRegion<?> region,
                final RegionedPushdownFilterContext context,
                final long costCeiling) {
            return allows(location, region, context) && ceilingAllows(costCeiling);
        }

        /**
         * Determine if this action is allowed based on the column region predicate.
         */
        private boolean allows(final ColumnRegion<?> region) {
            return regionAllows.test(region);
        }
    }
}
