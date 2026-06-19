//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
using Apache.Arrow;
using Apache.Arrow.Types;
using System.Diagnostics.CodeAnalysis;

namespace Deephaven.Dh_NetClient;

internal abstract class ColumnBuilder {
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

    if (TryMatchTypeToIListOfUnderlying(type, out var underlyingType)) {
      var miGeneric = typeof(ColumnBuilder).GetMethod(nameof(ForIListType)) ??
        throw new Exception($"Can't find {nameof(ForIListType)}");
      var miInstantiated = miGeneric.MakeGenericMethod(type, underlyingType);
      return (ColumnBuilder)miInstantiated.Invoke(null, [callerProvidedBuilder])!;
    }

    throw new Exception($"ColumnBuilder does not support type {Utility.FriendlyTypeName(type)}");
  }

  public static ColumnBuilder<T?> ForNullableType<T>(IArrowArrayBuilder? callerProvidedBuilder) where T : struct {
    var underlyingCb = ForType<T>(callerProvidedBuilder);
    return new NullableBuilder<T>(underlyingCb);
  }

  public static ColumnBuilder ForIListWithUnderlyingType(Type underlyingType) {
    var underlyingTypeToUse = underlyingType.IsValueType ? typeof(Nullable<>).MakeGenericType(underlyingType) : underlyingType;
    var ilistType = typeof(IList<>).MakeGenericType(underlyingTypeToUse);
    var miGeneric = typeof(ColumnBuilder).GetMethod(nameof(ForIListType)) ??
      throw new Exception($"Can't find {nameof(ForIListType)}");
    var miInstantiated = miGeneric.MakeGenericMethod(ilistType, underlyingTypeToUse);
    return (ColumnBuilder)miInstantiated.Invoke(null, [null])!;
  }

  public static ColumnBuilder<TList> ForIListType<TList, TUnderlying>(
    IArrowArrayBuilder? callerProvidedBuilder) where TList : class, IList<TUnderlying> {
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

  /// <summary>
  /// Is target an IList&lt;T&gt; or does it inherit from IList&lt;T&gt; for some T?
  /// If so, set underlying to T and return true. Otherwise return false.
  /// </summary>
  /// <param name="target">Type to examine</param>
  /// <param name="underlying">The underlying type if target is an IList&lt;T&gt;</param>
  /// <returns>True if target is an IList&lt;T&gt; or inherits from IList&lt;T&gt;, otherwise false</returns>

  private static bool TryMatchTypeToIListOfUnderlying(Type target,
    [MaybeNullWhen(false)] out Type underlying) {
    if (TryMatch(target, out underlying)) {
      return true;
    }
    foreach (var iface in target.GetInterfaces()) {
      if (TryMatch(iface, out underlying)) {
        return true;
      }
    }
    underlying = null;
    return false;

    // Is target an IList<T> for some T? If so, set underlying to T and return true. Otherwise return false.
    static bool TryMatch(Type target, [MaybeNullWhen(false)] out Type underlying) {
      if (target.IsGenericType && target.GetGenericTypeDefinition() == typeof(IList<>)) {
        underlying = target.GetGenericArguments()[0];
        return true;
      }
      underlying = null;
      return false;
    }
  }

  public abstract void AppendChunk(Chunk data, BooleanChunk nulls);
  public abstract Apache.Arrow.IArrowArray Build();
  public abstract (Apache.Arrow.Types.IArrowType, string, string?) GetTypeInfo();
  public abstract void AppendNull();
}

internal abstract class ColumnBuilder<T> : ColumnBuilder {
  public abstract void Append(T item);
}

internal sealed class TypicalBuilder<T, TArray, TBuilder> : ColumnBuilder<T>
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

  public override void AppendChunk(Chunk data, BooleanChunk nulls) {
    if (data.Size != nulls.Size) {
      throw new ArgumentException($"Chunk size {data.Size} does not match nulls size {nulls.Size}");
    }
    var typedChunk = data as Chunk<T>
      ?? throw new ArgumentException($"Expected chunk of type {Utility.FriendlyTypeName(typeof(Chunk<T>))}, but got {Utility.FriendlyTypeName(data.GetType())}");

    for (var i = 0; i != typedChunk.Size; i++) {
      if (nulls.Data[i]) {
        AppendNull();
      } else {
        Append(typedChunk.Data[i]);
      }
    }
  }

  public override (IArrowType, string, string?) GetTypeInfo() {
    return (_arrowType, _deephavenTypeName, null);
  }

  public override Apache.Arrow.IArrowArray Build() {
    return _builder.Build(null);
  }
}

internal sealed class CharColumnBuilder : ColumnBuilder<char> {
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

  public override void AppendChunk(Chunk data, BooleanChunk nulls) {
    if (data.Size != nulls.Size) {
      throw new ArgumentException($"Chunk size {data.Size} does not match nulls size {nulls.Size}");
    }
    var typedChunk = data as CharChunk
      ?? throw new ArgumentException($"Expected chunk of type {Utility.FriendlyTypeName(typeof(CharChunk))}, but got {Utility.FriendlyTypeName(data.GetType())}");

    for (var i = 0; i != typedChunk.Size; i++) {
      if (nulls.Data[i]) {
        AppendNull();
      } else {
        Append(typedChunk.Data[i]);
      }
    }
  }

  public override (IArrowType, string, string?) GetTypeInfo() {
    return (Apache.Arrow.Types.UInt16Type.Default, DeephavenMetadataConstants.Types.Char16, null);
  }

  public override Apache.Arrow.IArrowArray Build() {
    return _builder.Build();
  }
}

internal sealed class StringColumnBuilder : ColumnBuilder<string> {
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

  public override void AppendChunk(Chunk data, BooleanChunk nulls) {
    if (data.Size != nulls.Size) {
      throw new ArgumentException($"Chunk size {data.Size} does not match nulls size {nulls.Size}");
    }
    var typedChunk = data as StringChunk
      ?? throw new ArgumentException($"Expected chunk of type {Utility.FriendlyTypeName(typeof(StringChunk))}, but got {Utility.FriendlyTypeName(data.GetType())}");

    for (var i = 0; i != typedChunk.Size; i++) {
      if (nulls.Data[i]) {
        AppendNull();
      } else {
        Append(typedChunk.Data[i]!);
      }
    }
  }


  public override (IArrowType, string, string?) GetTypeInfo() {
    return (Apache.Arrow.Types.StringType.Default, DeephavenMetadataConstants.Types.String, null);
  }

  public override Apache.Arrow.IArrowArray Build() {
    return _builder.Build();
  }
}

internal sealed class NullableBuilder<T> : ColumnBuilder<T?> where T : struct {
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

  public override void AppendChunk(Chunk data, BooleanChunk nulls) {
    if (data.Size != nulls.Size) {
      throw new ArgumentException($"Chunk size {data.Size} does not match nulls size {nulls.Size}");
    }
    var typedChunk = data as Chunk<T?>
      ?? throw new ArgumentException($"Expected chunk of type {Utility.FriendlyTypeName(typeof(Chunk<T?>))}, but got {Utility.FriendlyTypeName(data.GetType())}");

    for (var i = 0; i != typedChunk.Size; i++) {
      if (nulls.Data[i]) {
        AppendNull();
      } else {
        Append(typedChunk.Data[i]);
      }
    }
  }

  public override Apache.Arrow.IArrowArray Build() {
    return _underlyingBuilder.Build();
  }

  public override (IArrowType, string, string?) GetTypeInfo() {
    return _underlyingBuilder.GetTypeInfo();
  }
}

internal sealed class ListBuilder<TList, TUnderlying> : ColumnBuilder<TList> where TList : class, IList<TUnderlying> {
  private readonly Apache.Arrow.ListArray.Builder _listBuilder;
  private readonly ColumnBuilder<TUnderlying> _underlyingBuilder;

  public ListBuilder(Apache.Arrow.ListArray.Builder listBuilder) {
    _listBuilder = listBuilder;
    _underlyingBuilder = ColumnBuilder.ForType<TUnderlying>(_listBuilder.ValueBuilder);
  }

  public override void Append(TList list) {
    _listBuilder.Append();
    foreach (var element in list) {
      if (element is not null) {
        _underlyingBuilder.Append(element);
      } else {
        _underlyingBuilder.AppendNull();
      }
    }
  }

  public override void AppendNull() {
    _listBuilder.AppendNull();
  }

  public override void AppendChunk(Chunk data, BooleanChunk nulls) {
    if (data.Size != nulls.Size) {
      throw new ArgumentException($"Chunk size {data.Size} does not match nulls size {nulls.Size}");
    }
    var typedChunk = data as ListChunk
      ?? throw new ArgumentException($"Expected chunk of type {typeof(ListChunk)}, but got {data.GetType()}");

    for (var i = 0; i != typedChunk.Size; i++) {
      if (nulls.Data[i]) {
        AppendNull();
      } else {
        var typedElement = typedChunk.Data[i] as TList ??
          throw new ArgumentException($"Expected element {i} to be of type {typeof(TList)}, but got {typedChunk.Data[i]?.GetType()}");
        Append(typedElement);
      }
    }
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
