package io.deephaven.db.v2.by;

import io.deephaven.configuration.Configuration;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.dbarrays.DbArray;
import io.deephaven.db.tables.select.SelectColumnFactory;
import io.deephaven.db.tables.utils.NameValidator;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.select.SelectColumn;
import com.tdunning.math.stats.TDigest;
import gnu.trove.list.array.TDoubleArrayList;

import java.util.ArrayList;
import java.util.List;

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
 * thrown. For tables with adds and removals you must use exact percentiles with
 * {@link ComboAggregateFactory#AggPct(double, java.lang.String...)}.
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
 * new ApproximatePercentile.PercentileDefinition("Latency").add(0.75, "L75").add(0.95, "L95").add(0.99, "L99")
 *         .nextColumn("Size").add(0.95, "S95").add(0.99, "S99");
 * final Table aggregated = ApproximatePercentile.approximatePercentiles(input, definition);
 * </pre>
 * </p>
 *
 * <p>
 * When parallelizing a workload, you may want to divide it based on natural partitioning and then compute an overall
 * percentile. In these cases, you should use the {@link PercentileDefinition#exposeDigest} method to expose the
 * internal t-digest structure as a column. If you then perform an array aggregation ({@link Table#by}), you can call
 * the {@link #accumulateDigests} function to produce a single digest that represents all of the constituent digests.
 * The amount of error introduced is related to the compression factor that you have selected for the digests. Once you
 * have a combined digest object, you can call the quantile or other functions to extract the desired percentile.
 * </p>
 */
public class ApproximatePercentile {
    public static double DEFAULT_COMPRESSION =
            Configuration.getInstance().getDoubleWithDefault("ApproximatePercentile.defaultCompression", 100.0);

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
    public static Table approximatePercentile(Table input, double percentile) {
        return approximatePercentile(input, DEFAULT_COMPRESSION, percentile,
                SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY);
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
    public static Table approximatePercentile(Table input, double percentile, String... groupByColumns) {
        return approximatePercentile(input, DEFAULT_COMPRESSION, percentile,
                SelectColumnFactory.getExpressions(groupByColumns));
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
    public static Table approximatePercentile(Table input, double percentile, SelectColumn... groupByColumns) {
        return approximatePercentile(input, DEFAULT_COMPRESSION, percentile, groupByColumns);
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
    public static Table approximatePercentile(Table input, double compression, double percentile,
            SelectColumn... groupByColumns) {
        final NonKeyColumnAggregationFactory aggregationContextFactory = new NonKeyColumnAggregationFactory(
                (type, resultName, exposeInternalColumns) -> new TDigestPercentileOperator(type, compression,
                        percentile, resultName));
        return ChunkedOperatorAggregationHelper.aggregation(aggregationContextFactory, (QueryTable) input,
                groupByColumns);
    }

    /**
     * A builder class for an approximate percentile definition to be used with {@link #approximatePercentiles}.
     */
    public static class PercentileDefinition {
        private final static PercentileDefinition[] ZERO_LENGTH_PERCENTILE_DEFINITION_ARRAY =
                new PercentileDefinition[0];

        private final PercentileDefinition prior;
        private final PercentileDefinition first;

        private final String inputColumn;
        private String digestColumnName;
        private final TDoubleArrayList percentiles;
        private final List<String> resultNames;

        double compression = DEFAULT_COMPRESSION;

        /**
         * Create a builder with the current input column set to inputColumn.
         *
         * @param inputColumn the current input column
         */
        public PercentileDefinition(String inputColumn) {
            this(inputColumn, null);
        }

        private PercentileDefinition(String inputColumn, PercentileDefinition prior) {
            this.inputColumn = inputColumn;
            percentiles = new TDoubleArrayList();
            resultNames = new ArrayList<>();
            digestColumnName = null;
            this.prior = prior;
            this.first = prior == null ? this : prior.first;
        }

        /**
         * Adds an output column.
         *
         * To set the inputColumn call {@link #nextColumn(String)}.
         *
         * @param percentile the percentile to calculate
         * @param resultName the result name
         *
         * @return a (possibly new) PercentileDefinition
         */
        public PercentileDefinition add(double percentile, String resultName) {
            percentiles.add(percentile);
            resultNames.add(NameValidator.validateColumnName(resultName));
            return this;
        }

        /**
         * Sets the name of the inputColumn
         *
         * @param inputColumn the name of the input column that subsequent calls to {@link #add} operate on.
         *
         * @return a (possibly new) PercentileDefinition
         */
        public PercentileDefinition nextColumn(String inputColumn) {
            return new PercentileDefinition(inputColumn, this);
        }

        /**
         * Sets the t-digest compression parameter.
         *
         * @param compression the t-digest compression factor.
         *
         * @return a (possibly new) PercentileDefinition
         */
        public PercentileDefinition setCompression(double compression) {
            first.compression = compression;
            return this;
        }

        /**
         * If true, the tDigest column is exposed using the given name
         *
         * @param digestColumnName the name of the t-digest column in the output
         *
         * @return a (possibly new) PercentileDefinition
         */
        public PercentileDefinition exposeDigest(String digestColumnName) {
            this.digestColumnName = digestColumnName;
            return this;
        }

        private static List<PercentileDefinition> flatten(PercentileDefinition value) {
            final List<PercentileDefinition> result = new ArrayList<>();
            value.flattenInto(result);
            return result;
        }

        private void flattenInto(List<PercentileDefinition> result) {
            if (prior != null) {
                prior.flattenInto(result);
            }
            result.add(this);
        }
    }

    /**
     * Compute a set of approximate percentiles for input according to the definitions in percentileDefinitions.
     *
     * @param input the table to compute approximate percentiles for
     * @param percentileDefinitions the compression factor, and map of input columns to output columns
     * @param groupByColumns the columns to group by
     * @return a table containing the groupByColumns and the approximate percentiles
     */
    public static Table approximatePercentiles(Table input, PercentileDefinition percentileDefinitions,
            SelectColumn... groupByColumns) {
        final List<PercentileDefinition> flatDefs = PercentileDefinition.flatten(percentileDefinitions);
        if (flatDefs.isEmpty()) {
            throw new IllegalArgumentException("No percentile columns defined!");
        }
        final double compression = flatDefs.get(0).compression;
        final NonKeyColumnAggregationFactory aggregationContextFactory =
                new NonKeyColumnAggregationFactory((type, resultName, exposeInternalColumns) -> {
                    for (final PercentileDefinition percentileDefinition : flatDefs) {
                        if (percentileDefinition.inputColumn.equals(resultName)) {
                            return new TDigestPercentileOperator(type, compression,
                                    percentileDefinition.digestColumnName, percentileDefinition.percentiles.toArray(),
                                    percentileDefinition.resultNames.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
                        }
                    }
                    return null;
                });
        return ChunkedOperatorAggregationHelper.aggregation(aggregationContextFactory, (QueryTable) input,
                groupByColumns);
    }

    /**
     * Compute a set of approximate percentiles for input according to the definitions in percentileDefinitions.
     *
     * @param input the table to compute approximate percentiles for
     * @param percentileDefinitions the compression factor, and map of input columns to output columns
     * @param groupByColumns the columns to group by
     * @return a table containing the groupByColumns and the approximate percentiles
     */
    public static Table approximatePercentiles(Table input, PercentileDefinition percentileDefinitions,
            String... groupByColumns) {
        return approximatePercentiles(input, percentileDefinitions, SelectColumnFactory.getExpressions(groupByColumns));
    }

    /**
     * Compute a set of approximate percentiles for input according to the definitions in percentileDefinitions.
     *
     * @param input the table to compute approximate percentiles for
     * @param percentileDefinitions the compression factor, and map of input columns to output columns
     * @return a table containing a single row with the the approximate percentiles
     */
    public static Table approximatePercentiles(Table input, PercentileDefinition percentileDefinitions) {
        return approximatePercentiles(input, percentileDefinitions, SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY);
    }

    /**
     * Accumulate an DbArray of TDigests into a single new TDigest.
     *
     * <p>
     * Accumulate the digests within the DbArray into a single TDigest. The compression factor is one third of the
     * compression factor of the first digest within the array. If the array has only a single element, then that
     * element is returned. If a null array is passed in, null is returned.
     * </p>
     *
     * <p>
     * This function is intended to be used for parallelization. The first step is to independently compute approximate
     * percentiles with an exposed digest column using your desired buckets. Next, call {@link Table#by(String...)} to
     * produce arrays of Digests for each relevant bucket. Once the arrays are created, use this function to accumulate
     * the arrays of digests within an {@link Table#update(String...)} statement. Finally, you may call the TDigest
     * quantile function (or others) to produce the desired approximate percentile.
     * </p>
     *
     * @param array an array of TDigests
     * @return the accumulated TDigests
     */
    public static TDigest accumulateDigests(DbArray<TDigest> array) {
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
