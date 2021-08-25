package io.deephaven.web.client.api.widget.plot;

import elemental2.dom.CustomEvent;
import elemental2.dom.CustomEventInit;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.DateWrapper;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.api.LongWrapper;
import io.deephaven.web.client.api.subscription.SubscriptionTableData;
import io.deephaven.web.client.api.subscription.TableSubscription;
import io.deephaven.web.client.fu.JsLog;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Represents the subscriptions that are needed to get data for the series in a figure. The equals and hashcode method
 * are designed to only match the features which can change while still maintaining the current subscription - the
 * actual "subscribed" series can change as long as they don't affect these other parameters.
 */
public final class FigureSubscription {
    /**
     * Minimum multiple that the table size needs to be decreased by in order to trigger downsampling. For example, if
     * there were 10k rows in the table, and downsampling might only reduce this to 6k, if the factor is 2 we would not
     * downsample. If there had been 15k rows, then with a factor of 2 we would downsample.
     */
    private static final int MIN_DOWNSAMPLE_FACTOR = 2;

    private final JsFigure figure;

    // The table that we are subscribing to
    private final JsTable originalTable;

    // The series that are currently watching this table - note that this can change, so is not included
    // in the hashcode/equals. What cannot change is the set of columns that are required by these series,
    // this must be checked by the method that mutates this set.
    private final Set<JsSeries> includedSeries = new HashSet<>();
    private final Set<String> requiredColumns;

    // Last, we apply downsample
    private final JsFigure.AxisRange downsampleAxisRange;
    private final JsFigure.DownsampleParams downsampleParams;

    // null until subscription is created
    private Promise<TableSubscription> subscription;
    private ChartData currentData;

    private boolean firstEventFired = false;

    public FigureSubscription(JsFigure figure, final JsTable originalTable,
            final JsFigure.AxisRange downsampleAxisRange, final JsFigure.DownsampleParams downsampleParams,
            final Set<JsSeries> series) {
        this.figure = figure;
        this.originalTable = originalTable;
        if (downsampleAxisRange == null) {
            assert downsampleParams == null : "Downsample not fully disabled";
        }
        this.downsampleAxisRange = downsampleAxisRange;
        this.downsampleParams = downsampleParams;

        this.includedSeries.addAll(series);
        this.requiredColumns =
                Collections.unmodifiableSet(includedSeries.stream().flatMap(s -> Arrays.stream(s.getSources()))
                        .map(source -> source.getDescriptor().getColumnName()).collect(Collectors.toSet()));
    }

    /**
     * Ensures that all of these series are present in this subscription, and returns the ones which were added, if any,
     * so they can get a notification of their current data.
     *
     * Replacement series must match this subscription already - the only facet that will be checked is that the columns
     * match.
     */
    public Set<JsSeries> replaceSeries(final Set<JsSeries> replacements) {
        final Set<JsSeries> copy = new HashSet<>(replacements);
        copy.removeAll(includedSeries);
        assert requiredColumns.containsAll(copy.stream().flatMap(s -> Arrays.stream(s.getSources()))
                .map(s -> s.getDescriptor().getColumnName()).collect(Collectors.toSet()));
        includedSeries.addAll(copy);

        // For each of the series in copy, if this subscription is downsampled we need to notify of this fact.
        if (downsampleAxisRange != null) {
            CustomEventInit init = CustomEventInit.create();
            init.setDetail(replacements.toArray());
            figure.fireEvent(JsFigure.EVENT_DOWNSAMPLESTARTED, init);
        }

        if (firstEventFired) {
            // Next, if any data has loaded, regardless of downsample state, we need to fire an update event for those
            // series
            CustomEventInit event = CustomEventInit.create();
            event.setDetail(new DataUpdateEvent(replacements.toArray(new JsSeries[0]), currentData, null));
            figure.fireEvent(JsFigure.EVENT_UPDATED, event);

            // Finally, if data was loaded and also the subscription is downsampled, we need to notify that the
            // downsample
            // is complete
            if (downsampleAxisRange != null) {
                CustomEventInit successInit = CustomEventInit.create();
                successInit.setDetail(replacements.toArray());
                figure.fireEvent(JsFigure.EVENT_DOWNSAMPLEFINISHED, successInit);
            }
        }

        return copy;
    }


    /*
     * Equality of two FigureSubscriptions is based on: o identity of the original table o equality of the oneclick
     * filter o equality of the downsampleAxisRange o equality of the downsampleParams EXCEPT the series array (since we
     * permit that to change) o equality of the required columns found in all includedSeries As such, the hashCode
     * should also be based on these.
     */

    @Override
    public int hashCode() {
        int result = figure.hashCode();
        result = 31 * result + originalTable.hashCode();// JsTable has no hashCode, so each instance should have its own
                                                        // value (until collisions happen)
        result = 31 * result + (downsampleAxisRange == null ? 0 : downsampleAxisRange.hashCode());
        result = 31 * result + (downsampleParams == null ? 0
                : downsampleParams.getPixelCount() * 31 + Arrays.hashCode(downsampleParams.getyCols()));
        result = 31 * result + requiredColumns.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof FigureSubscription)) {
            return false;
        }
        FigureSubscription other = (FigureSubscription) o;

        if (other.figure != figure) {
            return false;
        }
        if (other.originalTable != originalTable) {
            return false;
        }
        if (!Objects.equals(other.downsampleAxisRange, downsampleAxisRange)) {
            return false;
        }

        if ((other.downsampleParams == null && downsampleParams != null
                || other.downsampleParams != null && downsampleParams == null) // one is null and the other isnt
                || (other.downsampleParams != null && // now we know both are non-null, check if their two fields that
                                                      // we care about match
                        (other.downsampleParams.getPixelCount() != downsampleParams.getPixelCount()
                                || Arrays.equals(other.downsampleParams.getyCols(), downsampleParams.getyCols())))) {
            return false;
        }

        if (!Objects.equals(other.requiredColumns, requiredColumns)) {
            return false;
        }
        return true;
    }

    public void subscribe() {
        assert subscription == null;

        // Copy the table so we can optionally filter it - we'll need to release this table later

        // Assign the promise to the subscription field so we can kill it mid-work
        subscription = originalTable.copy(false).then(table -> {
            Promise<JsTable> tablePromise = Promise.resolve(table);

            // then downsample, subscribe
            if (downsampleAxisRange == null) {
                // subscribe to the table itself, we can't downsample for some reason

                // check if there are too many items, and downsampling isn't outright disabled
                if (table.getSize() > DownsampleOptions.MAX_SERIES_SIZE) {
                    if (includedSeries.stream().map(JsSeries::getDownsampleOptions)
                            .noneMatch(options -> options == DownsampleOptions.DISABLE)) {
                        // stop, we can't downsample, we haven't been told to disable, and there are too many items to
                        // fetch them outright
                        figure.downsampleNeeded("Disable downsampling to retrieve all items", includedSeries,
                                table.getSize());
                        // noinspection unchecked
                        return (Promise<TableSubscription>) (Promise) Promise.reject(
                                "Too many items to display, disable downsampling to display this series or size the axes");
                    } else if (table.getSize() > DownsampleOptions.MAX_SUBSCRIPTION_SIZE) {
                        figure.downsampleNeeded("Too many items to disable downsampling", includedSeries,
                                table.getSize());
                        // noinspection unchecked
                        return (Promise<TableSubscription>) (Promise) Promise
                                .reject("Too many items to disable downsampling");
                    }
                }

                // we actually sub to a copy, so that we can close it no matter what when we're done
                return subscribe(tablePromise);
            } else if (table.getSize() < 2 * (1 + downsampleParams.getyCols().length) * downsampleParams.getPixelCount()
                    * MIN_DOWNSAMPLE_FACTOR) {
                // Each "pixel" requires at most 2 rows per column in the table (max and min), so we check if the pixel
                // count times that is sufficiently
                // greater than the pixel size. The MIN_DOWNSAMPLE_FACTOR field is used to define "sufficiently greater"
                return subscribe(tablePromise);

                // TODO revisit this so we can watch the row count and downsample later if needed
            } else {
                final LongWrapper[] zoomRange;
                if (downsampleAxisRange.min != null && downsampleAxisRange.getMax() != null) {
                    zoomRange = new LongWrapper[] {DateWrapper.of(downsampleAxisRange.getMin()),
                            DateWrapper.of(downsampleAxisRange.getMax())};
                    JsLog.info("zoom range provided: ", zoomRange);
                } else {
                    zoomRange = null;
                }
                CustomEventInit init = CustomEventInit.create();
                init.setDetail(includedSeries.toArray());
                figure.fireEvent(JsFigure.EVENT_DOWNSAMPLESTARTED, init);
                Promise<JsTable> downsampled =
                        tablePromise.then(t -> t
                                .downsample(zoomRange, downsampleParams.getPixelCount(), downsampleAxisRange.getxCol(),
                                        downsampleParams.getyCols())
                                .then(resultTable -> Promise.resolve(resultTable), err -> {
                                    figure.downsampleFailed(err.toString(), includedSeries, table.getSize());

                                    if (table.getSize() > DownsampleOptions.MAX_SERIES_SIZE) {
                                        if (includedSeries.stream().map(JsSeries::getDownsampleOptions)
                                                .noneMatch(options -> options == DownsampleOptions.DISABLE)) {
                                            // stop, we can't downsample, we haven't been told to disable, and there are
                                            // too many items to fetch them outright
                                            // noinspection unchecked
                                            return (Promise<JsTable>) (Promise) Promise.reject("");
                                        }
                                    } else if (table.getSize() > DownsampleOptions.MAX_SUBSCRIPTION_SIZE) {
                                        // noinspection unchecked
                                        return (Promise<JsTable>) (Promise) Promise.reject("");
                                    }
                                    return Promise.resolve(table);
                                }));
                return subscribe(downsampled);
            }
        });
    }

    private Promise<TableSubscription> subscribe(final Promise<JsTable> tablePromise) {
        assert currentData == null;

        return tablePromise.then(table -> {
            if (subscription == null) {// do we need this?
                table.close();
                // noinspection unchecked
                return (Promise<TableSubscription>) (Promise) Promise.reject("Already closed");
            }

            TableSubscription sub = table.subscribe(table.getColumns());
            // TODO, technically we can probably unsubscribe to the table at this point, since we're listening to the
            // subscription itself

            this.currentData = new ChartData(table);
            sub.addEventListener(TableSubscription.EVENT_UPDATED, e -> {
                // refire with specifics for the columns that we're watching here, after updating data arrays
                SubscriptionTableData.UpdateEventData subscriptionUpdateData =
                        (SubscriptionTableData.UpdateEventData) ((CustomEvent) e).detail;
                currentData.update(subscriptionUpdateData);

                CustomEventInit event = CustomEventInit.create();
                event.setDetail(new DataUpdateEvent(includedSeries.toArray(new JsSeries[0]), currentData,
                        subscriptionUpdateData));
                figure.fireEvent(JsFigure.EVENT_UPDATED, event);

                if (!firstEventFired) {
                    firstEventFired = true;

                    if (downsampleAxisRange != null) {
                        CustomEventInit successInit = CustomEventInit.create();
                        successInit.setDetail(includedSeries.toArray());
                        figure.fireEvent(JsFigure.EVENT_DOWNSAMPLEFINISHED, successInit);
                    }
                }
            });
            return Promise.resolve(sub);
        });
    }

    public void unsubscribe() {
        assert subscription != null;
        // kill the subscription
        subscription.then(sub -> {
            sub.close();
            return null;
        });
        subscription = null;
    }


    public Set<JsSeries> getSeries() {
        return Collections.unmodifiableSet(includedSeries);
    }
}
