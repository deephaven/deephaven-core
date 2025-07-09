//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Io.Deephaven.Proto.Backplane.Grpc;

namespace Deephaven.Dh_NetClient;

/// <summary>
/// Represents a collection of Aggregate objects.
/// </summary>
public class AggregateCombo {
  /// <summary>
  /// Create an AggregateCombo
  /// </summary>
  /// <param name="list">The contained Aggregates</param>
  /// <returns></returns>
  public static AggregateCombo Create(params Aggregate[] list) {
    var aggregates = list.Select(elt => elt.Descriptor).ToArray();
    return new AggregateCombo(aggregates);
  }

  public readonly IReadOnlyList<ComboAggregateRequest.Types.Aggregate> Aggregates;

  private AggregateCombo(ComboAggregateRequest.Types.Aggregate[] aggregates) {
    Aggregates = aggregates;
  }
}

public class Aggregate {
  public readonly ComboAggregateRequest.Types.Aggregate Descriptor;

  public Aggregate(ComboAggregateRequest.Types.Aggregate descriptor) {
    Descriptor = descriptor;
  }

  /// <summary>
  /// Returns an aggregator that computes the total sum of absolute values, within an aggregation group,
  /// for each input column.
  /// </summary>
  /// <param name="columnSpecs">The source columns for the calculation</param>
  /// <returns>An Aggregate object representing the aggregation</returns>
  public static Aggregate AbsSum(params string[] columnSpecs) {
    return CreateAggForMatchPairs(ComboAggregateRequest.Types.AggType.AbsSum, columnSpecs);
  }

  /// <summary>
  /// Returns an aggregator that computes the average (mean) of values, within an aggregation group,
  /// for each input column.
  /// </summary>
  /// <param name="columnSpecs">The source columns for the calculation</param>
  /// <returns>An Aggregate object representing the aggregation</returns>
  public static Aggregate Avg(params string[] columnSpecs) {
    return CreateAggForMatchPairs(ComboAggregateRequest.Types.AggType.Avg, columnSpecs);
  }

  /// <summary>
  /// Returns an aggregator that computes the number of elements within an aggregation group,
  /// for each input column.
  /// </summary>
  /// <param name="columnSpec">The source column</param>
  /// <returns>An Aggregate object representing the aggregation</returns>
  public static Aggregate Count(string columnSpec) {
    var ad = CreateDescForColumn(ComboAggregateRequest.Types.AggType.Count, columnSpec);
    return new Aggregate(ad);
  }

  /// <summary>
  /// Returns an aggregator that computes the first value, within an aggregation group,
  /// for each input column.
  /// </summary>
  /// <param name="columnSpecs">The source columns for the calculation</param>
  /// <returns>An Aggregate object representing the aggregation</returns>
  public static Aggregate First(params string[] columnSpecs) {
    return CreateAggForMatchPairs(ComboAggregateRequest.Types.AggType.First, columnSpecs);
  }

  /// <summary>
  /// Returns an aggregator that computes an array of all values within an aggregation group,
  /// for each input column.
  /// </summary>
  /// <param name="columnSpecs">The source columns for the calculation</param>
  /// <returns>An Aggregate object representing the aggregation</returns>
  public static Aggregate Group(params string[] columnSpecs) {
    return CreateAggForMatchPairs(ComboAggregateRequest.Types.AggType.Group, columnSpecs);
  }

  /// <summary>
  /// Returns an aggregator that computes the last value, within an aggregation group,
  /// for each input column.
  /// </summary>
  /// <param name="columnSpecs">The source columns for the calculation</param>
  /// <returns>An Aggregate object representing the aggregation</returns>
  public static Aggregate Last(params string[] columnSpecs) {
    return CreateAggForMatchPairs(ComboAggregateRequest.Types.AggType.Last, columnSpecs);
  }

  /// <summary>
  /// Returns an aggregator that computes the maximum value, within an aggregation group,
  /// for each input column.
  /// </summary>
  /// <param name="columnSpecs">The source columns for the calculation</param>
  /// <returns>An Aggregate object representing the aggregation</returns>
  public static Aggregate Max(params string[] columnSpecs) {
    return CreateAggForMatchPairs(ComboAggregateRequest.Types.AggType.Max, columnSpecs);
  }

  /// <summary>
  /// Returns an aggregator that computes the median value, within an aggregation group,
  /// for each input column.
  /// </summary>
  /// <param name="columnSpecs">The source columns for the calculation</param>
  /// <returns>An Aggregate object representing the aggregation</returns>
  public static Aggregate Med(params string[] columnSpecs) {
    return CreateAggForMatchPairs(ComboAggregateRequest.Types.AggType.Median, columnSpecs);
  }

  /// <summary>
  /// Returns an aggregator that computes the minimum value, within an aggregation group,
  /// for each input column.
  /// </summary>
  /// <param name="columnSpecs">The source columns for the calculation</param>
  /// <returns>An Aggregate object representing the aggregation</returns>
  public static Aggregate Min(params string[] columnSpecs) {
    return CreateAggForMatchPairs(ComboAggregateRequest.Types.AggType.Min, columnSpecs);
  }

  /// <summary>
  /// Returns an aggregator that computes the designated percentile, within an aggregation group,
  /// for each input column.
  /// </summary>
  /// <param name="percentile">The designated percentile</param>
  /// <param name="avgMedian">When the percentile splits the group into two halves, whether to average the two middle values for the output value</param>
  /// <param name="columnSpecs">The source columns for the calculation</param>
  /// <returns>An Aggregate object representing the aggregation</returns>
  public static Aggregate Pct(double percentile, bool avgMedian, params string[] columnSpecs) {
    var pd = new ComboAggregateRequest.Types.Aggregate {
      Type = ComboAggregateRequest.Types.AggType.Percentile,
      Percentile = percentile,
      AvgMedian = avgMedian
    };
    pd.MatchPairs.AddRange(columnSpecs);
    return new Aggregate(pd);
  }

  /// <summary>
  /// Returns an aggregator that computes the sample standard deviation of values, within an
  /// aggregation group, for each input column.
  ///
  /// Sample standard deviation is computed using Bessel's correction (https://en.wikipedia.org/wiki/Bessel%27s_correction),
  /// which ensures that the sample variance will be an unbiased estimator of population variance.
  /// </summary>
  /// <param name="columnSpecs">The source columns for the calculation</param>
  /// <returns>An Aggregate object representing the aggregation</returns>
  public static Aggregate Std(params string[] columnSpecs) {
    return CreateAggForMatchPairs(ComboAggregateRequest.Types.AggType.Std, columnSpecs);
  }

  /// <summary>
  /// Returns an aggregator that computes the total sum of values, within an
  /// aggregation group, for each input column.
  /// </summary>
  /// <param name="columnSpecs">The source columns for the calculation</param>
  /// <returns>An Aggregate object representing the aggregation</returns>
  public static Aggregate Sum(params string[] columnSpecs) {
    return CreateAggForMatchPairs(ComboAggregateRequest.Types.AggType.Sum, columnSpecs);
  }

  /// <summary>
  /// Returns an aggregator that computes the sample variance of values, within an
  /// aggregation group, for each input column.
  ///
  /// Sample variance is computed using Bessel's correction (https://en.wikipedia.org/wiki/Bessel%27s_correction),
  /// which ensures that the sample variance will be an unbiased estimator of population variance.
  /// </summary>
  /// <param name="columnSpecs">The source columns for the calculation</param>
  /// <returns>An Aggregate object representing the aggregation</returns>
  public static Aggregate Var(params string[] columnSpecs) {
    return CreateAggForMatchPairs(ComboAggregateRequest.Types.AggType.Var, columnSpecs);
  }

  /// <summary>
  /// Returns an aggregator that computes the weighted average of values, within an
  /// aggregation group, for each input column.
  /// </summary>
  /// <param name="weightCol">The weight column for the calculation</param>
  /// <param name="columnSpecs">The source columns for the calculation</param>
  /// <returns>An Aggregate object representing the aggregation</returns>
  public static Aggregate WAvg(string weightCol, params string[] columnSpecs) {
    var desc = CreateDescForWeightedAverage(ComboAggregateRequest.Types.AggType.WeightedAvg,
      weightCol, columnSpecs);
    return new Aggregate(desc);
  }

  private static ComboAggregateRequest.Types.Aggregate
    CreateDescForWeightedAverage(ComboAggregateRequest.Types.AggType aggregateType,
      string weightCol, string[] columnSpecs) {
    var result = new ComboAggregateRequest.Types.Aggregate {
      ColumnName = weightCol,
      Type = aggregateType
    };
    result.MatchPairs.AddRange(columnSpecs);
    return result;
  }


  private static ComboAggregateRequest.Types.Aggregate
    CreateDescForMatchPairs(ComboAggregateRequest.Types.AggType aggregateType,
      string[] columnSpecs) {
    var result = new ComboAggregateRequest.Types.Aggregate {
      Type = aggregateType
    };
    result.MatchPairs.AddRange(columnSpecs);
    return result;
  }

  private static ComboAggregateRequest.Types.Aggregate CreateDescForColumn(
    ComboAggregateRequest.Types.AggType aggregateType, string columnSpec) {
    var result = new ComboAggregateRequest.Types.Aggregate {
      Type = aggregateType,
      ColumnName = columnSpec
    };
    return result;
  }

  private static Aggregate CreateAggForMatchPairs(ComboAggregateRequest.Types.AggType aggregateType,
    string[] columnSpecs) {
    var ad = CreateDescForMatchPairs(aggregateType, columnSpecs);
    return new Aggregate(ad);
  }

}
