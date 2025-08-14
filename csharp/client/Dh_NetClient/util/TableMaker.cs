//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Deephaven.Dh_NetClient;

public class TableMaker {
  private readonly List<ColumnInfo> _columnInfos = new();

  public void AddColumn<T>(string name, IReadOnlyList<T> values) {
    var cb = ColumnBuilder.ForType<T>(null);
    foreach (var value in values) {
      cb.Append(value);
    }
    var array = cb.Build();
    var (_, typeName, componentTypeName) = cb.GetTypeInfo();

    var kvMetadata = new List<KeyValuePair<string, string>>();
    kvMetadata.Add(KeyValuePair.Create(DeephavenMetadataConstants.Keys.Type, typeName));
    if (componentTypeName != null) {
      kvMetadata.Add(KeyValuePair.Create(DeephavenMetadataConstants.Keys.ComponentType, componentTypeName));
    }
    _columnInfos.Add(new ColumnInfo(name, array, kvMetadata.ToArray()));
  }

  public Apache.Arrow.Table ToArrowTable() {
    var schema = MakeSchema();
    var columns = MakeColumns();
    return new Apache.Arrow.Table(schema, columns);
  }

  public TableHandle MakeTable(TableHandleManager manager) {
    return Task.Run(() => MakeTableAsync(manager)).Result;
  }

  private async Task<TableHandle> MakeTableAsync(TableHandleManager manager) {
    var schema = MakeSchema();

    var server = manager.Server;

    var ticket = server.NewTicket();
    var flightDescriptor = ArrowUtil.ConvertTicketToFlightDescriptor(ticket);

    var headers = new Grpc.Core.Metadata();
    server.ForEachHeaderNameAndValue(headers.Add);

    var res = await server.FlightClient.StartPut(flightDescriptor, schema, headers);
    var data = GetColumnsNotEmpty();
    var numRows = data[^1].Length;

    var recordBatch = new Apache.Arrow.RecordBatch(schema, data, numRows);

    await res.RequestStream.WriteAsync(recordBatch);
    await res.RequestStream.CompleteAsync();

    while (await res.ResponseStream.MoveNext(CancellationToken.None)) {
      // TODO(kosak): find out whether it is necessary to eat values like this.
    }

    res.Dispose();
    return manager.MakeTableHandleFromTicket(ticket);
  }

  private Apache.Arrow.Schema MakeSchema() {
    ValidateSchema();

    var sb = new Apache.Arrow.Schema.Builder();
    foreach (var ci in _columnInfos) {
      var arrowType = ci.Data.Data.DataType;
      var field = new Apache.Arrow.Field(ci.Name, arrowType, true, ci.ArrowMetadata);
      sb.Field(field);
    }

    return sb.Build();
  }

  private Apache.Arrow.Column[] MakeColumns() {
    var result = new List<Apache.Arrow.Column>();

    foreach (var ci in _columnInfos) {
      var arrowType = ci.Data.Data.DataType;
      var field = new Apache.Arrow.Field(ci.Name, arrowType, true, ci.ArrowMetadata);
      result.Add(new Apache.Arrow.Column(field, [ci.Data]));
    }

    return result.ToArray();
  }

  private void ValidateSchema() {
    if (_columnInfos.Count == 0) {
      return;
    }

    var numRows = _columnInfos[0].Data.Length;
    for (var i = 1; i != _columnInfos.Count; ++i) {
      var ci = _columnInfos[i];
      if (ci.Data.Length != numRows) {
        var message =
          $"Column sizes not consistent: column 0 has size {numRows}, but column {i} has size {ci.Data.Length}";
        throw new Exception(message);
      }
    }
  }

  private Apache.Arrow.IArrowArray[] GetColumnsNotEmpty() {
    var result = _columnInfos.Select(ci => ci.Data).ToArray();
    if (result.Length == 0) {
      throw new Exception("Can't make table with no columns");
    }
    return result;
  }

  public string ToString(bool wantHeaders, bool wantLineNumbers = false) {
    var at = ToArrowTable();
    return ArrowUtil.Render(at, wantHeaders, wantLineNumbers);
  }

  public override string ToString() {
    return ToString(true);
  }

  private class ColumnBuilder {
    public static ColumnBuilder<T> ForType<T>(IArrowArrayBuilder? callerProvidedBuilder) {
      return (ColumnBuilder<T>)ForType(typeof(T), callerProvidedBuilder);
    }

    public static ColumnBuilder ForType(Type type, IArrowArrayBuilder? callerProvidedBuilder) {
      var nullableUnderlyingType = Nullable.GetUnderlyingType(type);
      if (nullableUnderlyingType != null) {
        var miGeneric = typeof(ColumnBuilder).GetMethod(nameof(ForNullableType)) ??
          throw new Exception($"Can't find {nameof(ForNullableType)}");
        var miInstantiated = miGeneric.MakeGenericMethod(nullableUnderlyingType);
        return (ColumnBuilder)miInstantiated.Invoke(null, [callerProvidedBuilder])!;
      }

      if (type == typeof(sbyte)) {
        var builderToUse =
          (IArrowArrayBuilder<sbyte, Int8Array, Int8Array.Builder>?)callerProvidedBuilder
          ?? new Apache.Arrow.Int8Array.Builder();
        return new TypicalBuilder<sbyte, Apache.Arrow.Int8Array, Apache.Arrow.Int8Array.Builder>(
          builderToUse, Apache.Arrow.Types.Int8Type.Default, DeephavenMetadataConstants.Types.Int8,
          DeephavenConstants.NullByte);
      }

      if (type == typeof(Int16)) {
        var builderToUse =
          (IArrowArrayBuilder<Int16, Int16Array, Int16Array.Builder>?)callerProvidedBuilder ??
          new Apache.Arrow.Int16Array.Builder();
        return new TypicalBuilder<Int16, Apache.Arrow.Int16Array, Apache.Arrow.Int16Array.Builder>(
          builderToUse, Apache.Arrow.Types.Int16Type.Default, DeephavenMetadataConstants.Types.Int16,
          DeephavenConstants.NullShort);
      }

      if (type == typeof(Int32)) {
        var builderToUse =
          (IArrowArrayBuilder<Int32, Int32Array, Int32Array.Builder>?)callerProvidedBuilder ??
          new Apache.Arrow.Int32Array.Builder();
        return new TypicalBuilder<Int32, Apache.Arrow.Int32Array, Apache.Arrow.Int32Array.Builder>(
          builderToUse, Apache.Arrow.Types.Int32Type.Default, DeephavenMetadataConstants.Types.Int32,
          DeephavenConstants.NullInt);
      }

      if (type == typeof(Int64)) {
        var builderToUse =
          (IArrowArrayBuilder<Int64, Int64Array, Int64Array.Builder>?)callerProvidedBuilder ?? 
          new Apache.Arrow.Int64Array.Builder();
        return new TypicalBuilder<Int64, Apache.Arrow.Int64Array, Apache.Arrow.Int64Array.Builder>(
          builderToUse, Apache.Arrow.Types.Int64Type.Default, DeephavenMetadataConstants.Types.Int64,
          DeephavenConstants.NullLong);
      }

      if (type == typeof(float)) {
        var builderToUse =
          (IArrowArrayBuilder<float, FloatArray, FloatArray.Builder>?)callerProvidedBuilder ??
          new Apache.Arrow.FloatArray.Builder();
        return new TypicalBuilder<float, Apache.Arrow.FloatArray, Apache.Arrow.FloatArray.Builder>(
          builderToUse, Apache.Arrow.Types.FloatType.Default, DeephavenMetadataConstants.Types.Float,
          DeephavenConstants.NullFloat);
      }

      if (type == typeof(double)) {
        var builderToUse =
          (IArrowArrayBuilder<double, DoubleArray, DoubleArray.Builder>?)callerProvidedBuilder ??
          new Apache.Arrow.DoubleArray.Builder();
        return new TypicalBuilder<double, Apache.Arrow.DoubleArray, Apache.Arrow.DoubleArray.Builder>(
          builderToUse, Apache.Arrow.Types.DoubleType.Default, DeephavenMetadataConstants.Types.Double,
          DeephavenConstants.NullDouble);
      }

      if (type == typeof(bool)) {
        var builderToUse =
          (IArrowArrayBuilder<bool, BooleanArray, BooleanArray.Builder>?)callerProvidedBuilder ??
          new Apache.Arrow.BooleanArray.Builder();
        return new TypicalBuilder<bool, Apache.Arrow.BooleanArray, Apache.Arrow.BooleanArray.Builder>(
          builderToUse, Apache.Arrow.Types.BooleanType.Default, DeephavenMetadataConstants.Types.Bool,
          null);
      }

      if (type == typeof(char)) {
        var builderToUse =
          (Apache.Arrow.UInt16Array.Builder?)callerProvidedBuilder ??
          new Apache.Arrow.UInt16Array.Builder();
        return new CharColumnBuilder(builderToUse);
      }

      if (type == typeof(string)) {
        var builderToUse =
          (Apache.Arrow.StringArray.Builder?)callerProvidedBuilder ??
          new Apache.Arrow.StringArray.Builder();
        return new StringColumnBuilder(builderToUse);
      }

      if (type == typeof(DateTimeOffset)) {
        var dataType = new Apache.Arrow.Types.TimestampType(TimeUnit.Nanosecond, "UTC");
        var builderToUse =
          (IArrowArrayBuilder<DateTimeOffset, TimestampArray, TimestampArray.Builder>?)callerProvidedBuilder ??
          new Apache.Arrow.TimestampArray.Builder(dataType);
        return new TypicalBuilder<DateTimeOffset, Apache.Arrow.TimestampArray, Apache.Arrow.TimestampArray.Builder>(
          builderToUse, dataType, DeephavenMetadataConstants.Types.DateTime, null);
      }

      if (type == typeof(DateOnly)) {
        var builderToUse =
          (IArrowArrayBuilder<DateOnly, Date64Array, Date64Array.Builder>?)callerProvidedBuilder ??
          new Apache.Arrow.Date64Array.Builder();
        return new TypicalBuilder<DateOnly, Apache.Arrow.Date64Array, Apache.Arrow.Date64Array.Builder>(
          builderToUse, Apache.Arrow.Types.Date64Type.Default, DeephavenMetadataConstants.Types.LocalDate,
          null);
      }

      if (type == typeof(TimeOnly)) {
        var builderToUse =
          (IArrowArrayBuilder<TimeOnly, Time64Array, Time64Array.Builder>?)callerProvidedBuilder ??
          new Apache.Arrow.Time64Array.Builder();
        return new TypicalBuilder<TimeOnly, Apache.Arrow.Time64Array, Apache.Arrow.Time64Array.Builder>(
          builderToUse, Apache.Arrow.Types.Time64Type.Default, DeephavenMetadataConstants.Types.LocalTime,
          null);
      }

      var listUnderlyingType = GetIListInterfaceUnderlyingType(type);
      if (listUnderlyingType != null) {
        var miGeneric = typeof(ColumnBuilder).GetMethod(nameof(ForIListType)) ??
          throw new Exception($"Can't find {nameof(ForIListType)}");
        var miInstantiated = miGeneric.MakeGenericMethod(type, listUnderlyingType);
        return (ColumnBuilder)miInstantiated.Invoke(null, [callerProvidedBuilder])!;
      }

      throw new Exception($"ColumnBuilder does not support type {Utility.FriendlyTypeName(type)}");
    }
    
    public static ColumnBuilder<T?> ForNullableType<T>(IArrowArrayBuilder? callerProvidedBuilder) where T : struct {
      var underlyingCb = ForType<T>(callerProvidedBuilder);
      return new NullableBuilder<T>(underlyingCb);
    }

    public static ColumnBuilder<TList> ForIListType<TList, TUnderlying>(
      IArrowArrayBuilder? callerProvidedBuilder) where TList : IList<TUnderlying> {
      Apache.Arrow.ListArray.Builder builderToUse;
      if (callerProvidedBuilder == null) {
        // Make a temporary column builder just so I can get the correct Arrow data type
        var tempCb = ForType<TUnderlying>(null);
        var (underlyingArrowType, _, _) = tempCb.GetTypeInfo();
        builderToUse = new Apache.Arrow.ListArray.Builder(underlyingArrowType);
      } else {
        builderToUse = (Apache.Arrow.ListArray.Builder)callerProvidedBuilder;
      }
      return new ListBuilder<TList, TUnderlying>(builderToUse);
    }

    private static Type? GetIListInterfaceUnderlyingType(Type ilistType) {
      var temp = ilistType.GetInterfaces();
      return ilistType.GetInterfaces().Select(GetIListUnderlyingType).FirstOrDefault(t => t != null);
    }

    private static Type? GetIListUnderlyingType(Type ilistType) {
      if (ilistType.IsGenericType && !ilistType.IsGenericTypeDefinition) {
        // Instantiated generic type only
        var genericType = ilistType.GetGenericTypeDefinition();
        if (ReferenceEquals(genericType, typeof(IList<>))) {
          return ilistType.GetGenericArguments()[0];
        }
      }
      return null;
    }
  }

  private abstract class ColumnBuilder<T> : ColumnBuilder {
    public abstract void Append(T item);
    public abstract void AppendNull();

    public abstract Apache.Arrow.IArrowArray Build();

    public abstract (Apache.Arrow.Types.IArrowType, string, string?) GetTypeInfo();
  }

  private sealed class TypicalBuilder<T, TArray, TBuilder> : ColumnBuilder<T>
    where T : struct, IEquatable<T>
    where TArray : Apache.Arrow.IArrowArray
    where TBuilder : Apache.Arrow.IArrowArrayBuilder<TArray> {
    private readonly Apache.Arrow.IArrowArrayBuilder<T, TArray, TBuilder> _builder;
    private readonly Apache.Arrow.Types.IArrowType _arrowType;
    private readonly string _deephavenTypeName;
    private readonly T? _deephavenNullValue;

    public TypicalBuilder(Apache.Arrow.IArrowArrayBuilder<T, TArray, TBuilder> builder,
      Apache.Arrow.Types.IArrowType arrowType, string deephavenTypeName, T? deephavenNullValue) {
      _builder = builder;
      _arrowType = arrowType;
      _deephavenTypeName = deephavenTypeName;
      _deephavenNullValue = deephavenNullValue;
    }

    public override void Append(T item) {
      if (_deephavenNullValue.HasValue && _deephavenNullValue.Value.Equals(item)) {
        _builder.AppendNull();
      } else {
        _builder.Append(item);
      }
    }

    public override void AppendNull() {
      _builder.AppendNull();
    }

    public override (IArrowType, string, string?) GetTypeInfo() {
      return (_arrowType, _deephavenTypeName, null);
    }

    public override Apache.Arrow.IArrowArray Build() {
      return _builder.Build(null);
    }
  }

  private sealed class CharColumnBuilder : ColumnBuilder<char> {
    private readonly Apache.Arrow.UInt16Array.Builder _builder;

    public CharColumnBuilder(UInt16Array.Builder builder) {
      _builder = builder;
    }

    public override void Append(char item) {
      if (item == DeephavenConstants.NullChar) {
        _builder.AppendNull();
      } else {
        _builder.Append(item);
      }
    }

    public override void AppendNull() {
      _builder.AppendNull();
    }

    public override (IArrowType, string, string?) GetTypeInfo() {
      return (Apache.Arrow.Types.UInt16Type.Default, DeephavenMetadataConstants.Types.Char16, null);
    }

    public override Apache.Arrow.IArrowArray Build() {
      return _builder.Build();
    }
  }

  private sealed class StringColumnBuilder : ColumnBuilder<string> {
    private readonly Apache.Arrow.StringArray.Builder _builder;

    public StringColumnBuilder(StringArray.Builder builder) {
      _builder = builder;
    }

    public override void Append(string item) {
      _builder.Append(item);
    }

    public override void AppendNull() {
      _builder.AppendNull();
    }

    public override (IArrowType, string, string?) GetTypeInfo() {
      return (Apache.Arrow.Types.StringType.Default, DeephavenMetadataConstants.Types.String, null);
    }

    public override Apache.Arrow.IArrowArray Build() {
      return _builder.Build();
    }
  }

  private sealed class NullableBuilder<T> : ColumnBuilder<T?> where T : struct {
    private readonly ColumnBuilder<T> _underlyingBuilder;

    public NullableBuilder(ColumnBuilder<T> underlyingBuilder) {
      _underlyingBuilder = underlyingBuilder;
    }

    public override void Append(T? item) {
      if (item.HasValue) {
        _underlyingBuilder.Append(item.Value);
      } else {
        _underlyingBuilder.AppendNull();
      }
    }

    public override void AppendNull() {
      _underlyingBuilder.AppendNull();
    }

    public override Apache.Arrow.IArrowArray Build() {
      return _underlyingBuilder.Build();
    }

    public override (IArrowType, string, string?) GetTypeInfo() {
      return _underlyingBuilder.GetTypeInfo();
    }
  }

  private class ListBuilder<TList, TUnderlying> : ColumnBuilder<TList> where TList : IList<TUnderlying> {
    private readonly Apache.Arrow.ListArray.Builder _listBuilder;
    private readonly ColumnBuilder<TUnderlying> _underlyingBuilder;

    public ListBuilder(Apache.Arrow.ListArray.Builder listBuilder) {
      _listBuilder = listBuilder;
      _underlyingBuilder = ColumnBuilder.ForType<TUnderlying>(_listBuilder.ValueBuilder);
    }

    public override void Append(TList list) {
      _listBuilder.Append();
      foreach (var element in list) {
        _underlyingBuilder.Append(element);
      }
    }

    public override void AppendNull() {
      _listBuilder.AppendNull();
    }

    public override IArrowArray Build() {
      return _listBuilder.Build();
    }

    public override (IArrowType, string, string?) GetTypeInfo() {
      var (underlyingArrowType, underlyingDeephavenType, _) = _underlyingBuilder.GetTypeInfo();

      var arrowType = new Apache.Arrow.Types.ListType(underlyingArrowType);
      var deephavenType = underlyingDeephavenType + "[]";
      var componentType = underlyingDeephavenType;
      return (arrowType, deephavenType, componentType);
    }
  }

  private record ColumnInfo(string Name,
    Apache.Arrow.IArrowArray Data,
    KeyValuePair<string, string>[] ArrowMetadata);
}
