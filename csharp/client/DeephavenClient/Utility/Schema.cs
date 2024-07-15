using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Deephaven.DeephavenClient.Utility;

/**
 * These values need to be kept in sync with the corresponding values on the C++ side.
 */
internal enum ElementTypeId {
  Char,
  Int8,
  Int16,
  Int32,
  Int64,
  Float,
  Double,
  Bool,
  String,
  Timestamp,
  List
};

public class Schema {
  public Int32 NumCols => Names.Length;
  public Int64 NumRows;
  public string[] Names { get; }
  internal ElementTypeId[] Types { get; }
  private readonly Dictionary<string, Int32> _nameToIndex;

  internal Schema(string[] names, int[] elementTypesAsInt, Int64 numRows) {
    if (names.Length != elementTypesAsInt.Length) {
      throw new ArgumentException($"names.Length ({names.Length}) != types.Length({elementTypesAsInt.Length})");
    }
    Names = names;
    Types = elementTypesAsInt.Select(elt => (ElementTypeId)elt).ToArray();
    _nameToIndex = Names.Select((name, idx) => new { name, idx })
      .ToDictionary(elt => elt.name, elt => elt.idx);
    NumRows = numRows;
  }

  public Int32 GetColumnIndex(string name) {
    if (TryGetColumnIndex(name, out var result)) {
      return result;
    }

    throw new ArgumentException($"""Column name "{name}" not found""");
  }

  public bool TryGetColumnIndex(string name, out Int32 result) {
    return _nameToIndex.TryGetValue(name, out result);
  }
}
