//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
namespace Deephaven.Dh_NetClient;

public static class ChunkMaker {
  public static Chunk CreateChunkFor(IColumnSource columnSource, int chunkSize) {
    var visitor = new ChunkMakerVisitor(chunkSize);
    columnSource.Accept(visitor);
    return visitor.Result!;
  }

  private class ChunkMakerVisitor(int chunkSize) :
    IColumnSourceVisitor<ICharColumnSource>,
    IColumnSourceVisitor<IByteColumnSource>,
    IColumnSourceVisitor<IInt16ColumnSource>,
    IColumnSourceVisitor<IInt32ColumnSource>,
    IColumnSourceVisitor<IInt64ColumnSource>,
    IColumnSourceVisitor<IFloatColumnSource>,
    IColumnSourceVisitor<IDoubleColumnSource>,
    IColumnSourceVisitor<IBooleanColumnSource>,
    IColumnSourceVisitor<IStringColumnSource>,
    IColumnSourceVisitor<IDateTimeOffsetColumnSource>,
    IColumnSourceVisitor<IDateOnlyColumnSource>,
    IColumnSourceVisitor<ITimeOnlyColumnSource> {
    public Chunk? Result { get; private set; }

    public void Visit(ICharColumnSource cs) => Make(cs);
    public void Visit(IByteColumnSource cs) => Make(cs);
    public void Visit(IInt16ColumnSource cs) => Make(cs);
    public void Visit(IInt32ColumnSource cs) => Make(cs);
    public void Visit(IInt64ColumnSource cs) => Make(cs);
    public void Visit(IFloatColumnSource cs) => Make(cs);
    public void Visit(IDoubleColumnSource cs) => Make(cs);
    public void Visit(IBooleanColumnSource cs) => Make(cs);
    public void Visit(IStringColumnSource cs) => Make(cs);
    public void Visit(IDateTimeOffsetColumnSource cs) => Make(cs);
    public void Visit(IDateOnlyColumnSource cs) => Make(cs);
    public void Visit(ITimeOnlyColumnSource cs) => Make(cs);

    public void Visit(IColumnSource cs) {
      throw new Exception($"Assertion failed: No visitor for type {Utility.FriendlyTypeName(cs.GetType())}");
    }

    private void Make<T>(IColumnSource<T> _) {
      Result = Chunk<T>.Create(chunkSize);
    }
  }
}
