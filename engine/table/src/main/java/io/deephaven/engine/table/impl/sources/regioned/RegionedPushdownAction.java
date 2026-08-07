//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.util.SafeCloseable;

import javax.annotation.Nullable;
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

    /**
     * A predicate that can be used to determine if a pushdown action is valid for a given table location and column
     * region.
     */
    public interface LocationRegionPredicate {
        boolean test(@Nullable TableLocation location, @Nullable ColumnRegion<?> region);
    }

    // Note: the disabled static configuration values are _not_ currently final and we must test them on each call
    // to ensure that dynamic configuration changes are respected.
    protected final BooleanSupplier disabled;

    private final long filterCost;
    private final Predicate<RegionedPushdownFilterContext> contextAllows;
    private final LocationRegionPredicate locationRegionAllows;

    private RegionedPushdownAction(
            final BooleanSupplier disabled,
            final long filterCost,
            final Predicate<RegionedPushdownFilterContext> contextAllows,
            final LocationRegionPredicate locationRegionAllows) {
        this.disabled = Objects.requireNonNull(disabled);
        this.filterCost = filterCost;
        this.contextAllows = Objects.requireNonNull(contextAllows);
        this.locationRegionAllows = Objects.requireNonNull(locationRegionAllows);
    }

    /**
     * The cost of performing this action.
     */
    public long filterCost() {
        return filterCost;
    }

    /**
     * Determine if this action is allowed for the given table location, column region, and the filter context
     * (including comparison against already-executed filter costs).
     */
    public boolean allows(
            final TableLocation location,
            final ColumnRegion<?> region,
            final RegionedPushdownFilterContext context) {
        return !disabled.getAsBoolean()
                && contextAllows(context)
                && locationRegionAllows.test(location, region);
    }

    /**
     * Determine if this action is allowed for the given table location, column region, filter context (including
     * comparison against already-executed filter costs), and the cost ceiling.
     */
    public boolean allows(
            final TableLocation location,
            final ColumnRegion<?> region,
            final RegionedPushdownFilterContext context,
            final long costCeiling) {
        return allows(location, region, context) && ceilingAllows(costCeiling);
    }

    /**
     * Determine if this action is allowed based on the cost ceiling.
     */
    private boolean ceilingAllows(final long costCeiling) {
        return filterCost <= costCeiling;
    }

    /**
     * Determine if this action is allowed based on the already-executed filter cost and the filter context predicate.
     */
    private boolean contextAllows(final RegionedPushdownFilterContext context) {
        return context.executedFilterCost() < filterCost && contextAllows.test(context);
    }

    /**
     * A marker class to indicate that this action is performed by a region.
     */
    public static final class Region extends RegionedPushdownAction {
        public Region(
                final BooleanSupplier disabled,
                final long filterCost,
                final Predicate<RegionedPushdownFilterContext> contextAllows,
                final LocationRegionPredicate locationRegionAllows) {
            super(disabled, filterCost, contextAllows, locationRegionAllows);
        }
    }

    /**
     * A marker class to indicate that this action is performed by a table location.
     */
    public static final class Location extends RegionedPushdownAction {
        public Location(
                final BooleanSupplier disabled,
                final long filterCost,
                final Predicate<RegionedPushdownFilterContext> contextAllows,
                final LocationRegionPredicate locationRegionAllows) {
            super(disabled, filterCost, contextAllows, locationRegionAllows);
        }
    }
}
