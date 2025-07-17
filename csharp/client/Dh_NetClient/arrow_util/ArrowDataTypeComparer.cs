//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Apache.Arrow.Types;

namespace Deephaven.Dh_NetClient;

public sealed class ArrowDataTypeComparer :
  IArrowTypeVisitor<Int8Type>,
  IArrowTypeVisitor<Int16Type>,
  IArrowTypeVisitor<Int32Type>,
  IArrowTypeVisitor<Int64Type>,
  IArrowTypeVisitor<FloatType>,
  IArrowTypeVisitor<DoubleType>,
  IArrowTypeVisitor<UInt16Type>,
  IArrowTypeVisitor<StringType>,
  IArrowTypeVisitor<BooleanType>,
  IArrowTypeVisitor<TimestampType>,
  IArrowTypeVisitor<Date64Type>,
  IArrowTypeVisitor<Time64Type>,
  IArrowTypeVisitor<ListType> {
  private readonly IArrowType _self;
  public bool Result = false;

  public ArrowDataTypeComparer(IArrowType self) {
    _self = self;
  }

  public void Visit(IArrowType type) {
    throw new Exception($"Don't recognize type {Utility.FriendlyTypeName(type.GetType())}");
  }

  public void Visit(Int8Type other) {
    Result = _self is Int8Type;
  }

  public void Visit(Int16Type other) {
    Result = _self is Int16Type;
  }

  public void Visit(Int32Type other) {
    Result = _self is Int32Type;
  }

  public void Visit(Int64Type other) {
    Result = _self is Int64Type;
  }

  public void Visit(FloatType other) {
    Result = _self is FloatType;
  }

  public void Visit(DoubleType other) {
    Result = _self is DoubleType;
  }

  public void Visit(UInt16Type other) {
    Result = _self is UInt16Type;
  }

  public void Visit(StringType other) {
    Result = _self is StringType;
  }

  public void Visit(BooleanType other) {
    Result = _self is BooleanType;
  }

  public void Visit(TimestampType other) {
    Result = _self is TimestampType typedSelf &&
      typedSelf.Timezone == other.Timezone &&
      typedSelf.Unit == other.Unit;
  }

  public void Visit(Date64Type other) {
    Result = _self is Date64Type typedSelf &&
      typedSelf.Unit == other.Unit;
  }

  public void Visit(Time64Type other) {
    Result = _self is Time64Type typedSelf &&
      typedSelf.Unit == other.Unit;
  }

  public void Visit(ListType other) {
    if (_self is not ListType typedSelf) {
      Result = false;
      return;
    }

    var nestedVisitor = new ArrowDataTypeComparer(typedSelf.ValueDataType);
    other.ValueDataType.Accept(nestedVisitor);
    Result = nestedVisitor.Result;
  }
}
