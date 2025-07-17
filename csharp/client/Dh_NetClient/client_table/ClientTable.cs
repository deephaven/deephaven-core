//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;

namespace Deephaven.Dh_NetClient;

public interface IClientTable : IDisposable {
  Apache.Arrow.Table ToArrowTable() {
    return ArrowUtil.ToArrowTable(this);
  }

  string ToString(bool wantHeaders, bool wantLineNumbers) {
    var at = ToArrowTable();
    return ArrowUtil.Render(at, wantHeaders, wantLineNumbers);
  }

  /// <summary>
  /// Get the RowSequence (in position space) that underlies this Table.
  /// </summary>
  /// <returns>The RowSequence</returns>
  RowSequence RowSequence { get; }

  /// <summary>
  /// Gets a ColumnSource from the ClientTable by index
  /// </summary>
  /// <param name="columnIndex"></param>
  /// <returns>The ColumnSource</returns>
  IColumnSource GetColumn(int columnIndex);

  /// <summary>
  /// Gets a ColumnSource from the ClientTable by name.
  /// </summary>
  /// <param name="name">The name of the column</param>
  /// <returns>The ColumnSource, if 'name' was found. Otherwise, throws an exception.</returns>
  public sealed IColumnSource GetColumn(string name) {
    _ = TryGetColumnInternal(name, true, out var result);
    return result!;
  }

  /// <summary>
  /// Gets a ColumnSource from the ClientTable by name.
  /// </summary>
  /// <param name="name">The name of the column</param>
  /// <param name="result">The column, if found</param>
  /// <returns>True if 'name' was found, false otherwise.</returns>
  public sealed bool TryGetColumn(string name, [NotNullWhen(true)] out IColumnSource? result) {
    return TryGetColumnInternal(name, false, out result);
  }

  private bool TryGetColumnInternal(string name, bool strict, [NotNullWhen(true)] out IColumnSource? result) {
    if (TryGetColumnIndex(name, out var index)) {
      result = GetColumn(index);
      return true;
    }

    if (strict) {
      throw new Exception($"Column {name} was not found");
    }

    result = null;
    return false;
  }

  /// <summary>
  /// Gets the index of a ColumnSource from the ClientTable by name.
  /// </summary>
  /// <param name="name">The name of the column</param>
  /// <param name="result">The column index, if found</param>
  /// <returns>True if 'name' was found, false otherwise.</returns>
  public sealed bool TryGetColumnIndex(string name, out int result) {
    result = Schema.GetFieldIndex(name);
    return result >= 0;
  }

  /// <summary>
  /// Number of rows in the ClienTTable
  /// </summary>
  Int64 NumRows { get; }

  /// <summary>
  /// Number of columns in the ClienTTable
  /// </summary>
  Int64 NumCols { get; }

  /// <summary>
  /// The ClientTable Schema
  /// </summary>
  Schema Schema { get; }
}
