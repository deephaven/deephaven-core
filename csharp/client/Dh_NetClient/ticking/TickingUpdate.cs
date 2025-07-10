//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
namespace Deephaven.Dh_NetClient;

public class TickingUpdate(
  IClientTable prev,
  RowSequence removedRowsIndexSpace,
  IClientTable afterRemoves,
  RowSequence addedRowsIndexSpace,
  IClientTable afterAdds,
  RowSequence[] modifiedRowsIndexSpace,
  IClientTable afterModifies) {
  public IClientTable Prev => prev;
  public RowSequence RemovedRowsIndexSpace => removedRowsIndexSpace;
  public IClientTable AfterRemoves => afterRemoves;
  public RowSequence AddedRowsIndexSpace => addedRowsIndexSpace;
  public IClientTable AfterAdds => afterAdds;
  public RowSequence[] ModifiedRowsIndexSpace => modifiedRowsIndexSpace;
  public IClientTable AfterModifies => afterModifies;

  public IClientTable Current => afterModifies;
}
