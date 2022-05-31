package io.deephaven.engine.table.impl.by;

import com.tdunning.math.stats.TDigest;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.spec.AggSpecApproximatePercentile;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.vector.ObjectVector;

/**
 * Generate approximate percentile aggregations of a table.
 *
 * <p>
 * The underlying data structure and algorithm used is a t-digest as described at https://github.com/tdunning/t-digest,
 * which has a "compression" parameter that determines the size of the retained values. From the t-digest documentation,
 * &quot;100 is a common value for normal uses. 1000 is extremely large. The number of centroids retained will be a
 * smallish (usually less than 10) multiple of this number.&quote;
 * </p>
 *
 * <p>
 * All input columns are cast to doubles and the result columns are doubles.
 * </p>
 *
 * <p>
 * The input table must be add only, if modifications or removals take place; then an UnsupportedOperationException is
 * thrown. For tables with adds and removals you must use exact percentiles with {@link Aggregation#AggPct}.
 * </p>
 *
 * <p>
 * You may compute either one approximate percentile or several approximate percentiles at once. For example, to compute
 * the 95th percentile of all other columns, by the "Sym" column you would call:
 * 
 * <pre>
 * ApproximatePercentile.approximatePercentile(input, 0.95, "Sym")
 * </pre>
 * </p>
 *
 * <p>
 * If you need to compute several percentiles, it is more efficient to compute them simultaneously. For example, this
 * example computes the 75th, 95th, and 99th percentiles of the "Latency" column using a builder pattern, and the 95th
 * and 99th percentiles of the "Size" column by "Sym":
 * 
 * <pre>
 * final Table aggregated = input.aggBy(List.of(
 *         Aggregation.ApproxPct("Latency', PctOut(0.75, "L75"), PctOut(0.95, "L95"), PctOut(0.99, "L99")
 *         Aggregation.ApproxPct("Size', PctOut(0.95, "S95"), PctOut(0.99, "S99")));
 * </pre>
 * </p>
 *
 * <p>
 * When parallelizing a workload, you may want to divide it based on natural partitioning and then compute an overall
 * percentile. In these cases, you should use the {@link Aggregation#AggTDigest} aggregation to expose the internal
 * t-digest structure as a column. If you then perform an array aggregation ({@link Table#groupBy}), you can call the
 * {@link #accumulateDigests} function to produce a single digest that represents all of the constituent digests. The
 * amount of error introduced is related to the compression factor that you have selected for the digests. Once you have
 * a combined digest object, you can call the quantile or other functions to extract the desired percentile.
 * </p>
 */
public class ApproximatePercentile {

    // static usage only
    private ApproximatePercentile() {}

    /**
     * Compute the approximate percentiles for the table.
     *
     * @param input the input table
     * @param percentile the percentile to compute for each column
     * @return a single row table with double columns representing the approximate percentile for each column of the
     *         input table
     */
    public static Table approximatePercentileBy(Table input, double percentile) {
        return input.aggAllBy(AggSpecApproximatePercentile.of(percentile));
    }

    /**
     * Compute the approximate percentiles for the table.
     *
     * @param input the input table
     * @param percentile the percentile to compute for each column
     * @param groupByColumns the columns to group by
     * @return a with the groupByColumns and double columns representing the approximate percentile for each remaining
     *         column of the input table
     */
    public static Table approximatePercentileBy(Table input, double percentile, String... groupByColumns) {
        return input.aggAllBy(AggSpecApproximatePercentile.of(percentile), groupByColumns);
    }

    /**
     * Compute the approximate percentiles for the table.
     *
     * @param input the input table
     * @param percentile the percentile to compute for each column
     * @param groupByColumns the columns to group by
     * @return a with the groupByColumns and double columns representing the approximate percentile for each remaining
     *         column of the input table
     */
    public static Table approximatePercentileBy(Table input, double percentile, SelectColumn... groupByColumns) {
        return input.aggAllBy(AggSpecApproximatePercentile.of(percentile), groupByColumns);
    }

    /**
     * Compute the approximate percentiles for the table.
     *
     * @param input the input table
     * @param compression the t-digest compression parameter
     * @param percentile the percentile to compute for each column
     * @param groupByColumns the columns to group by
     * @return a with the groupByColumns and double columns representing the approximate percentile for each remaining
     *         column of the input table
     */
    public static Table approximatePercentileBy(Table input, double compression, double percentile,
            SelectColumn... groupByColumns) {
        return input.aggAllBy(AggSpecApproximatePercentile.of(percentile, compression), groupByColumns);
    }

    /**
     * Accumulate a Vector of TDigests into a single new TDigest.
     *
     * <p>
     * Accumulate the digests within the Vector into a single TDigest. The compression factor is one third of the
     * compression factor of the first digest within the array. If the array has only a single element, then that
     * element is returned. If a null array is passed in, null is returned.
     * </p>
     *
     * <p>
     * This function is intended to be used for parallelization. The first step is to independently expose a T-Digest
     * aggregation column with the appropriate compression factor on each of a set of sub-tables, using
     * {@link Aggregation#AggTDigest} and {@link Table#aggBy}. Next, call {@link Table#groupBy(String...)} to produce
     * arrays of Digests for each relevant bucket. Once the arrays are created, use this function to accumulate the
     * arrays of digests within an {@link Table#update} statement. Finally, you may call the TDigest quantile function
     * (or others) to produce the desired approximate percentile.
     * </p>
     *
     * @param array an array of TDigests
     * @return the accumulated TDigests
     */
    public static TDigest accumulateDigests(ObjectVector<TDigest> array) {
        if (array == null) {
            return null;
        }
        if (array.size() == 1) {
            return array.get(0);
        }
        final TDigest digest = TDigest.createDigest(array.get(0).compression() / 3);
        for (int ii = 0; ii < array.size(); ++ii) {
            digest.add(array.get(ii));
        }
        return digest;
    }
}
